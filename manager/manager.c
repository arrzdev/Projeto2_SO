#include "logging.h"
#include "wire_protocol.h"
#include "utils/client.h"

static void print_usage()
{
  fprintf(stderr, "usage: \n"
                  "   manager <register_pipe_name> <pipe_name> create <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> remove <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> list\n");
}

int createBox(char *register_pipe_name, char *client_pipe_name, char *box_name)
{
  // send create request by connecting to server with CREATE_BOX op_code
  if (connect(CREATE_BOX, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    printf("Error connecting to server");
    return -1;
  }

  OP_CODE_SIZE op_code;
  RETURN_CODE_SIZE return_code;
  char error_message[MESSAGE_SIZE];

  // open client_fifo to receive server response
  int client_fifo = open(client_pipe_name, O_RDONLY);

  // listen for response
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  if (read(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error reading response from server");
    return -1;
  }

  sscanf(wire_message, "%hhd|%d|%[^\n]", &op_code, &return_code, error_message);

  // check if the message is directed to us (should be but..)
  if (op_code != RETURN_CREATE_BOX)
  {
    printf("Received misplaced response from server");
    return -1;
  }

  // print response
  if (return_code == 0)
    fprintf(stdout, "OK\n");
  else
    fprintf(stdout, "ERROR %s\n", error_message);

  // close client fifo
  close(client_fifo);

  // unlink client fifo
  unlink(client_pipe_name);

  return 0;
}

int deleteBox(char *register_pipe_name, char *pipe_name, char *box_name)
{
  // make fifo
  if (mkfifo(pipe_name, 0666) == -1)
  {
    printf("Error creating fifo");
    return -1;
  }

  char wire_message[PROTOCOL_MESSAGE_SIZE];
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s|%s", DELETE_BOX, pipe_name, box_name);

  // open register fifo
  int register_fifo = open(register_pipe_name, O_WRONLY);

  // send wire message to register client
  if (write(register_fifo, wire_message, strlen(wire_message) + 1) == -1)
  {
    printf("Error registering publisher");
    return -1;
  };

  printf("Connected to server:\n \t- %s\n", wire_message);

  OP_CODE_SIZE op_code;
  RETURN_CODE_SIZE error_code;
  char error_message[PROTOCOL_MESSAGE_SIZE];

  int pipe = open(pipe_name, O_RDONLY);

  // listen for response
  while (1)
  {
    if (read(pipe, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error reading response from server");
      return -1;
    }

    sscanf(wire_message, "%hhd|%d|%[^\n]", &op_code, &error_code, error_message);

    if (op_code != RETURN_DELETE_BOX)
      continue;

    break;
  }

  printf("%s\n", wire_message);

  return 0;
}

int listBox(char *register_pipe_name, char *pipe_name)
{
  // make fifo
  if (mkfifo(pipe_name, 0666) == -1)
  {
    printf("Error creating fifo");
    return -1;
  }

  char wire_message[PROTOCOL_MESSAGE_SIZE];
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s", LIST_BOXES, pipe_name);

  // open register fifo
  int register_fifo = open(register_pipe_name, O_WRONLY);

  // send wire message to register client
  if (write(register_fifo, wire_message, strlen(wire_message) + 1) == -1)
  {
    printf("Error registering publisher");
    return -1;
  };

  printf("Connected to server:\n \t- %s\n", wire_message);

  OP_CODE_SIZE op_code;
  uint8_t last;
  char box_name[32];
  uint64_t box_size;
  uint64_t pubs;
  uint64_t subs;

  int pipe = open(pipe_name, O_RDONLY);

  // listen for response
  while (1)
  {
    if (read(pipe, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error reading response from server");
      return -1;
    }

    sscanf(wire_message, "%hhd|%hhd|%s|%lu|%lu|%lu", &op_code, &last, box_name, &box_size, &pubs, &subs);

    if (op_code != RETURN_LIST_BOXES)
      continue;

    printf("%s\n", wire_message);
  }

  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 4)
  {
    printf("too few arguments\n");
    return -1;
  }

  char *register_pipe_name = argv[1];
  char *command = argv[3];

  if (strcmp(command, "create") == 0)
  {
    if (argc != 5)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];
    char *box_name = argv[4];

    return createBox(register_pipe_name, pipe_name, box_name);
  }
  else if (strcmp(command, "delete") == 0)
  {
    if (argc != 5)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];
    char *box_name = argv[4];

    return deleteBox(register_pipe_name, pipe_name, box_name);
  }
  else if (strcmp(command, "list") == 0)
  {
    if (argc != 4)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];

    return listBox(register_pipe_name, pipe_name);
  }
  else
  {
    print_usage();
    return -1;
  }

  return 0;
}
