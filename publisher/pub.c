#include "logging.h"
#include "client.h"
#include "wire_protocol.h"

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

  // connect to server
  if (connect(REGISTER_PUBLISHER, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    WARN("error connecting to server");
    return -1;
  }

  sleep(1);

  if (access(client_pipe_name, F_OK) != 0)
  {
    WARN("Error occured\n");
    return -1;
  }

  // open the client fifo
  int client_fifo = open(client_pipe_name, O_WRONLY);

  char buffer[MESSAGE_SIZE];
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  while (fgets(buffer, MESSAGE_SIZE, stdin) != NULL)
  {
    // create wire message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s", SEND_MESSAGE, buffer);

    // check if fifo is open
    if (access(client_pipe_name, F_OK) != 0)
    {
      WARN("Error sending message\n");
      return -1;
    }

    // send wire message to server using client pipe
    if (write(client_fifo, wire_message, strlen(wire_message) + 1) == -1)
    {
      // close client fifo
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);

      WARN("Error sending message\n");
      return -1;
    };
  }

  // close fifo
  if (close(client_fifo) == -1)
  {
    WARN("Error closing fifo %s\n", client_pipe_name);
    return -1;
  }

  // unlink fifo
  unlink(client_pipe_name);
  return 0;
}
