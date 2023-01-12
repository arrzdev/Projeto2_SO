#include "logging.h"
#include "client.h"
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

  // connect to server
  if (connect(REGISTER_PUBLISHER, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    WARN("error connecting to server");
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

    // if stdin is EOF disconnect client
    if (feof(stdin))
      break;

    // send wire message to server using client pipe
    if (write(client_fifo, wire_message, strlen(wire_message) + 1) == -1)
    {
      // close client fifo
      close(client_fifo);
      printf("Error sending message\n");
      return -1;
    };

    printf("Sent: %s\n", buffer);
  }

  // close fifo
  close(client_fifo);

  // unlink fifo
  unlink(client_pipe_name);
  return 0;
}
