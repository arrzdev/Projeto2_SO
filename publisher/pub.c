#include "logging.h"
#include "wire_protocol.h"
#include "string.h"

#define OP_CODE 1

int main(int argc, char **argv)
{
  if (argc != 4)
  {
    WARN("number of arguments invalid");
    return -1;
  }

  char *register_pipe_name = argv[1];
  char *client_pipe_name = argv[2];
  char *box_name = argv[3];

  char wire_message[MAX_WIRE_MESSAGE_SIZE];
  snprintf(wire_message, MAX_WIRE_MESSAGE_SIZE, "%d|%s|%s", OP_CODE, client_pipe_name, box_name);

  // send wire message to register fifo
  // open fifo
  int register_fifo = open(register_pipe_name, O_WRONLY);

  // send wire message
  if (write(register_fifo, wire_message, strlen(wire_message) + 1) == -1)
  {
    printf("Error registering publisher");
    return -1;
  };

  printf("Publisher Sent: %s\n", wire_message);

  // close fifo
  close(register_fifo);

  return 0;
}
