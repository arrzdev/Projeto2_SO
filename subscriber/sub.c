#include "logging.h"
#include "client.h"
#include "wire_protocol.h"
#include <signal.h>

volatile sig_atomic_t disconnect_flag = 0;

static void handleSIGINT(int sig)
{
  (void)sig;
  disconnect_flag = 1;
}

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
  if (connect(REGISTER_SUBSCRIBER, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    WARN("error connecting to server");
    return -1;
  }

  // open the client fifo
  int client_fifo = open(client_pipe_name, O_RDONLY);

  char buffer[PROTOCOL_MESSAGE_SIZE];

  // setup signal handler
  signal(SIGINT, handleSIGINT);

  while (1)
  {
    if (disconnect_flag)
      break;

    if (read(client_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == 0)
      break;

    // print message
    fprintf(stdout, "%s\n", buffer);
  }

  printf("Disconnected...\n");

  // close fifo
  close(client_fifo);

  // unlink fifo
  unlink(client_pipe_name);
  return 0;
}