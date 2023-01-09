#include "logging.h"
#include "wire_protocol.h"
#include "string.h"

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

  char wire_message[PROTOCOL_MESSAGE_SIZE];
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s|%s", REGISTER_PUBLISHER, client_pipe_name, box_name);

  // create the client pipe
  if (mkfifo(client_pipe_name, 0666) == -1)
  {
    perror("error creating client pipe");
    return -1;
  }

  // open register fifo
  int register_fifo = open(register_pipe_name, O_WRONLY);

  // send wire message to register client
  if (write(register_fifo, wire_message, strlen(wire_message) + 1) == -1)
  {
    printf("Error registering publisher");
    return -1;
  };

  printf("Connected to server:\n \t- %s\n", wire_message);

  // close register fifo
  close(register_fifo);

  // TODO: the process above is the same for every client (op_code is different)
  // so this can be abstracted into a function
  char message[MESSAGE_SIZE];
  while (1)
  {
    if (fgets(message, MESSAGE_SIZE, stdin) == NULL || strlen(message) == EOF)
    {
      // end connection
      // delete client pipe
      unlink(client_pipe_name);
      printf("Closed connection");
      return -1;
    }

    // create wire message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s", SEND_MESSAGE, message);

    // send wire message to server using client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, strlen(wire_message) + 1) == -1)
    {
      printf("Error sending message\n");
      return -1;
    };

    printf("Sent: %s\n", message);
  }

  return 0;
}
