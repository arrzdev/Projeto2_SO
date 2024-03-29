#include "wire_protocol.h"
#include "logging.h"
// TODO: we could change the way the protocol works by still using strings, but having aux functions to parse the wire messages and return response structures with the data making it easy and more readble to work with trough the code

int connect(OP_CODE_SIZE op_code, char *register_pipe_name, char *client_pipe_name, char *box_name)
{
  // create the client pipe
  if (mkfifo(client_pipe_name, 0666) == -1)
  {
    perror("error creating client pipe");
    return -1;
  }

  // create wire message
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s|%s", op_code, client_pipe_name, box_name);

  // open register fifo
  int register_fifo = open(register_pipe_name, O_WRONLY);

  // send wire message to register client
  if (write(register_fifo, wire_message, strlen(wire_message) + 1) == -1)
  {
    close(register_fifo);
    WARN("Error registering publisher");
    unlink(client_pipe_name);
    return -1;
  };

  // close register fifo
  close(register_fifo);

  return 0;
}