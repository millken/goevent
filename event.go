package goevent

import (
	"context"
	"sync"
	"sync/atomic"
)

var (
	_subscribers     = make(map[string]eventChannelSlice)
	_subscribersLock = new(sync.RWMutex)
	_stats           = make(map[string]*uint64)
)

// Event holds topic name and data.
type Event struct {
	Data  any
	Topic string
	wg    *sync.WaitGroup
}

// Done calls Done on sync.WaitGroup if set.
func (e *Event) Done() {
	if e.wg != nil {
		e.wg.Done()
	}
}

// EventChannel is a channel which can accept an Event.
type EventChannel chan Event

// NewEventChannel Creates a new EventChannel.
func NewEventChannel() EventChannel {
	return make(EventChannel)
}

// dataChannelSlice is a slice of DataChannels.
type eventChannelSlice []EventChannel

// CallbackFunc is a function that is called when a message is published to a topic.
type CallbackFunc func(ctx context.Context, topic string, args any)

// getSubscribingChannels returns all subscribing channels including wildcard matches.
func getSubscribingChannels(topic string) eventChannelSlice {
	subChannels := eventChannelSlice{}

	for topicName := range _subscribers {
		if topicName == topic || matchWildcard(topicName, topic) {
			subChannels = append(subChannels, _subscribers[topicName]...)
		}
	}

	return subChannels
}

// Publish data to a topic and wait for all subscribers to finish
// This function creates a waitGroup internally. All subscribers must call Done() function on Event.
func Publish(topic string, args any) {
	wg := sync.WaitGroup{}
	_subscribersLock.Lock()
	subscribers := getSubscribingChannels(topic)
	_subscribersLock.Unlock()
	wg.Add(len(subscribers))
	doPublish(
		subscribers,
		Event{
			Data:  args,
			Topic: topic,
			wg:    &wg,
		})
	wg.Wait()
	atomic.AddUint64(topicStats(topic), 1)
}

// PublishOnce same as Publish but makes sure only published once on topic.
func PublishOnce(topic string, data interface{}) {
	if atomic.LoadUint64(topicStats(topic)) > 0 {
		return
	}

	Publish(topic, data)
}

// PublishAsync data to a topic asynchronously
// This function returns a bool channel which indicates that all subscribers where called.
func PublishAsync(topic string, args any) {
	_subscribersLock.Lock()
	subscribers := getSubscribingChannels(topic)
	_subscribersLock.Unlock()
	doPublish(
		subscribers,
		Event{
			Data:  args,
			Topic: topic,
			wg:    nil,
		})
	atomic.AddUint64(topicStats(topic), 1)
}

// Subscribe to a topic passing a EventChannel.
func Subscribe(topic string) EventChannel {
	ch := make(EventChannel)
	SubscribeChannel(topic, ch)

	return ch
}

// SubscribeChannel subscribes to a given Channel.
func SubscribeChannel(topic string, ch EventChannel) {
	_subscribersLock.Lock()
	defer _subscribersLock.Unlock()

	if prev, found := _subscribers[topic]; found {
		_subscribers[topic] = append(prev, ch)
	} else {
		_subscribers[topic] = append([]EventChannel{}, ch)
	}
}

// SubscribeCallback provides a simple wrapper that allows to directly register CallbackFunc instead of channels.
func SubscribeCallback(ctx context.Context, topic string, fn CallbackFunc) {
	ch := NewEventChannel()
	SubscribeChannel(topic, ch)

	go func(ctx context.Context, callable CallbackFunc) {
		evt := <-ch
		fn(ctx, evt.Topic, evt.Data)
		evt.Done()
	}(ctx, fn)
}

// doPublish is publishing events to channels internally.
func doPublish(channels eventChannelSlice, evt Event) {
	go func(channels eventChannelSlice, evt Event) {
		for _, ch := range channels {
			ch <- evt
		}
	}(channels, evt)
}

// Code from https://github.com/minio/minio/blob/master/pkg/wildcard/match.go
func matchWildcard(pattern, name string) bool {
	if pattern == "" {
		return name == pattern
	}

	if pattern == "*" {
		return true
	}
	// Does only wildcard '*' match.
	return deepMatchRune([]rune(name), []rune(pattern), true)
}

// Code from https://github.com/minio/minio/blob/master/pkg/wildcard/match.go
func deepMatchRune(str, pattern []rune, simple bool) bool { //nolint:unparam
	for len(pattern) > 0 {
		switch pattern[0] {
		default:
			if len(str) == 0 || str[0] != pattern[0] {
				return false
			}
		case '*':
			return deepMatchRune(str, pattern[1:], simple) ||
				(len(str) > 0 && deepMatchRune(str[1:], pattern, simple))
		}

		str = str[1:]

		pattern = pattern[1:]
	}

	return len(str) == 0 && len(pattern) == 0
}

func topicStats(topicName string) *uint64 {
	_, ok := _stats[topicName]
	if !ok {
		_stats[topicName] = new(uint64)
	}

	return _stats[topicName]
}
