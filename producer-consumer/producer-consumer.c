#include "producer-consumer.h"
#include <stdlib.h>
#include <stdio.h>
#include "wire_protocol.h"
/*
pc_queue_t is treated as a circular buffer.

This means that we keep track of the head and tail of the array, and we can
use the modulo operator to wrap around the array.

This allows for O(1) enqueue and dequeue operations.
*/

int has_priority(void *element)
{
  if(element == NULL)
    return 0;

  char *str = (char *)element;

  int first_char = str[0] - '0';

  return first_char == REGISTER_PUBLISHER;
}

int pcq_create(pc_queue_t *queue, size_t capacity)
{
  if (queue == NULL)
  {
    return -1;
  }

  // Allocate memory for the buffer
  queue->pcq_buffer = (void **)malloc(capacity * sizeof(char *));

  if (queue->pcq_buffer == NULL)
  {
    return -1;
  }

  // initialize the queue for circular buffer
  queue->pcq_capacity = capacity;
  queue->pcq_current_size = 0;

  queue->pcq_head = capacity - 1;
  queue->pcq_tail = capacity - 1;

  // Initialize the mutex and condition variables
  if (pthread_mutex_init(&queue->pcq_current_size_lock, NULL) != 0)
    return -1;
  if (pthread_mutex_init(&queue->pcq_head_lock, NULL) != 0)
    return -1;
  if (pthread_mutex_init(&queue->pcq_tail_lock, NULL) != 0)
    return -1;
  if (pthread_mutex_init(&queue->pcq_pusher_condvar_lock, NULL) != 0)
    return -1;
  if (pthread_mutex_init(&queue->pcq_popper_condvar_lock, NULL) != 0)
    return -1;
  if (pthread_cond_init(&queue->pcq_pusher_condvar, NULL) != 0)
    return -1;
  if (pthread_cond_init(&queue->pcq_popper_condvar, NULL) != 0)
    return -1;

  return 0;
}

int pcq_destroy(pc_queue_t *queue)
{
  if (queue == NULL)
  {
    return -1;
  }

  // Free the buffer
  free(queue->pcq_buffer);

  // Destroy the mutex and condition variables
  if (pthread_mutex_destroy(&queue->pcq_current_size_lock) != 0)
    return -1;
  if (pthread_mutex_destroy(&queue->pcq_head_lock) != 0)
    return -1;
  if (pthread_mutex_destroy(&queue->pcq_tail_lock) != 0)
    return -1;
  if (pthread_mutex_destroy(&queue->pcq_pusher_condvar_lock) != 0)
    return -1;
  if (pthread_mutex_destroy(&queue->pcq_popper_condvar_lock) != 0)
    return -1;
  if (pthread_cond_destroy(&queue->pcq_pusher_condvar) != 0)
    return -1;
  if (pthread_cond_destroy(&queue->pcq_popper_condvar) != 0)
    return -1;

  return 0;
}

int pcq_enqueue(pc_queue_t *queue, void *elem)
{
  if (queue == NULL)
  {
    return -1;
  }

  printf("enqueue %s\n", (char *)elem);
  
  // Lock the mutex
  if (pthread_mutex_lock(&queue->pcq_popper_condvar_lock) != 0)
    return -1;

  // Wait until the queue has an open spot
  // queue only has open spot if "pop" action happens
  while (queue->pcq_current_size == queue->pcq_capacity)
  {
    // wait for pusher condvar to be changed
    if (pthread_cond_wait(&queue->pcq_popper_condvar, &queue->pcq_popper_condvar_lock) != 0)
      return -1;
  }

  // Lock the current_size mutex
  if (pthread_mutex_lock(&queue->pcq_current_size_lock) != 0)
    return -1;

  // Lock the head mutex
  if (pthread_mutex_lock(&queue->pcq_head_lock) != 0)
    return -1;

  // Lock the tail mutex
  if (pthread_mutex_lock(&queue->pcq_tail_lock) != 0)
    return -1;

  int priority_el = has_priority(elem);

  if (priority_el)
  {
    // this value start has value after tail
    size_t next_pos;

    if (queue->pcq_tail == 0)
      next_pos = queue->pcq_capacity - 1;
    else
      next_pos = queue->pcq_tail - 1;

    queue->pcq_buffer[next_pos] = elem;

    // put this element is next_pos, if prev element has priority swap untill not
    int i = 0;

    while(i < queue->pcq_current_size) {
      if (has_priority(queue->pcq_buffer[next_pos])) {
        void *temp = queue->pcq_buffer[next_pos];
        queue->pcq_buffer[next_pos] = queue->pcq_buffer[queue->pcq_tail];
        queue->pcq_buffer[queue->pcq_tail] = temp;
      }
      next_pos = (next_pos + 1) % queue->pcq_capacity;
      i++;
    }

    queue->pcq_tail = next_pos;
  }
  else
  {
    // Add the element to the queue
    queue->pcq_buffer[queue->pcq_head] = elem;

    // Update head value
    queue->pcq_head = (queue->pcq_head + 1) % queue->pcq_capacity;
  }

  // Increase the current size
  queue->pcq_current_size++;

  if (pthread_mutex_unlock(&queue->pcq_tail_lock) != 0)
    return -1;

  // Unlock the tail mutex
  if (pthread_mutex_unlock(&queue->pcq_head_lock) != 0)
    return -1;

  // Unlock the current_size mutex
  if (pthread_mutex_unlock(&queue->pcq_current_size_lock) != 0)
    return -1;

  // Unlock
  if (pthread_mutex_unlock(&queue->pcq_popper_condvar_lock) != 0)
    return -1;

  // signal pusher cond var
  if (pthread_cond_signal(&queue->pcq_pusher_condvar) != 0)
    return -1;

  printf("Added to queue: %s\n", (char *)elem);

  printf("Head: %ld\n", queue->pcq_head);
  printf("Tail: %ld\n", queue->pcq_tail);

  // print all elements in queue
  int i = 0;
  while (i < queue->pcq_capacity)
  {
    printf("queue[%d] = %s\n", i, (char *)queue->pcq_buffer[i]);
    i++;
  }

  return 0;
}

void *pcq_dequeue(pc_queue_t *queue)
{
  if (queue == NULL)
  {
    return NULL;
  }

  // Lock the mutex
  if (pthread_mutex_lock(&queue->pcq_pusher_condvar_lock) != 0)
    return NULL;

  // Wait until the queue has an open spot
  // queue only has open spot if "pop" action happens
  while (queue->pcq_current_size == 0)
  {
    // wait for pusher condvar to be changed
    if (pthread_cond_wait(&queue->pcq_pusher_condvar, &queue->pcq_pusher_condvar_lock) != 0)
      return NULL;
  }

  // Lock the current_size mutex
  if (pthread_mutex_lock(&queue->pcq_current_size_lock) != 0)
    return NULL;

  // Lock the tail mutex
  if (pthread_mutex_lock(&queue->pcq_tail_lock) != 0)
    return NULL;

  // Remove the element from the queue
  void *elem = queue->pcq_buffer[queue->pcq_tail];
  queue->pcq_buffer[queue->pcq_tail] = NULL;
  // Update tail value
  queue->pcq_tail = (queue->pcq_tail + 1) % queue->pcq_capacity;

  // Decrease the current size
  queue->pcq_current_size--;

  // Unlock the tail mutex
  if (pthread_mutex_unlock(&queue->pcq_tail_lock) != 0)
    return NULL;

  // Unlock the current_size mutex
  if (pthread_mutex_unlock(&queue->pcq_current_size_lock) != 0)
    return NULL;

  
  // Unlock
  if (pthread_mutex_unlock(&queue->pcq_pusher_condvar_lock) != 0)
    return NULL;

  // signal popper cond var
  if (pthread_cond_signal(&queue->pcq_popper_condvar) != 0)
    return NULL;

  return elem;
}