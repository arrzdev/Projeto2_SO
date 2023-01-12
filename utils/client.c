#include "wire_protocol.h"

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
    printf("Error registering publisher");
    unlink(client_pipe_name);
    return -1;
  };

  printf("Connected to server:\n \t- %s\n", wire_message);

  // close register fifo
  close(register_fifo);

  return 0;
}