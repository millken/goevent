package goevent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublishAsync(t *testing.T) {
	const testTopicName = "foo:bar"

	ch1 := NewEventChannel()
	SubscribeChannel(testTopicName, ch1)
	ch2 := Subscribe("foo:*")

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		evt := <-ch1
		if evt.Topic != testTopicName {
			t.Fail()
		}

		if evt.Data != "bar" { // nolint:goconst
			t.Fail()
		}

		wg.Done()
	}() //nolint:wsl,nolintlint

	go func() {
		evt := <-ch2
		if evt.Topic != testTopicName {
			t.Fail()
		}

		if evt.Data != "bar" {
			t.Fail()
		}

		wg.Done()
	}() //nolint:wsl,nolintlint

	PublishAsync(testTopicName, "bar")

	wg.Wait()

	assert.Equal(t, uint64(1), atomic.LoadUint64(topicStats(testTopicName)))
}

func TestPublish(t *testing.T) {
	const testTopicName = "foo1:bar"

	ch1 := NewEventChannel()
	SubscribeChannel(testTopicName, ch1)
	ch2 := Subscribe("foo1:*")

	inc := new(uint64)
	go func() {
		evt := <-ch1
		if evt.Topic != testTopicName {
			t.Errorf("expected topic %s, got %s", testTopicName, evt.Topic)
		}

		if evt.Data != "bar" {
			t.Errorf("expected data %s, got %v %T", "bar", evt.Data, evt.Data)
		}

		atomic.AddUint64(inc, 1)
		evt.Done()
	}()

	go func() {
		evt := <-ch2
		if evt.Topic != testTopicName {
			t.Fail()
		}

		if evt.Data != "bar" {
			t.Fail()
		}

		atomic.AddUint64(inc, 1)
		evt.Done()
	}()

	Publish(testTopicName, "bar")

	if atomic.LoadUint64(inc) != uint64(2) {
		t.Fail()
	}

	assert.Equal(t, uint64(1), atomic.LoadUint64(topicStats(testTopicName)))

	// Try to republish with publish once
	PublishOnce(testTopicName, "bar")

	// Count should be still 1
	assert.Equal(t, uint64(1), atomic.LoadUint64(topicStats(testTopicName)))
}

func TestSubscribeCallback(t *testing.T) {
	const testTopicName = "foo2:bar"

	inc := new(uint64)

	SubscribeCallback(context.Background(), testTopicName, func(_ context.Context, topic string, data any) {
		if topic != testTopicName {
			t.Fail()
		}

		if data != "bar" {
			t.Errorf("expected data %s, got %v %T", "bar", data, data)
		}
		atomic.AddUint64(inc, 1)
	})

	SubscribeCallback(context.Background(), "foo2:*", func(_ context.Context, topic string, data any) {
		if topic != testTopicName {
			t.Fail()
		}

		if data != "bar" {
			t.Fail()
		}
		atomic.AddUint64(inc, 1)
	})

	Publish(testTopicName, "bar")

	if atomic.LoadUint64(inc) != uint64(2) {
		t.Fail()
	}

	// Try to republish with publish once
	PublishOnce(testTopicName, "bar")

	// Count should be still 1
	assert.Equal(t, uint64(1), atomic.LoadUint64(topicStats(testTopicName)))
}
