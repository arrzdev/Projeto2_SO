#include "logging.h"

#include "wire_protocol.h"
#include "string.h"

static void print_usage()
{
  fprintf(stderr, "usage: \n"
                  "   manager <register_pipe_name> <pipe_name> create <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> remove <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> list\n");
}

int createBox(char *register_pipe_name, char *pipe_name, char *box_name)
{
  // make fifo
  if (mkfifo(pipe_name, 0666) == -1)
  {
    printf("Error creating fifo");
    return -1;
  }

  char wire_message[PROTOCOL_MESSAGE_SIZE];
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%s|%s", CREATE_BOX, pipe_name, box_name);

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

    if (op_code != RETURN_CREATE_BOX)
      continue;

    break;
  }

  printf("%s\n", wire_message);

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

    return create_box(register_pipe_name, pipe_name, box_name);
  }
  else if(strcmp(command, "delete") == 0)
  {
    if (argc != 5)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];
    char *box_name = argv[4];

    return delete_box(register_pipe_name, pipe_name, box_name);
  }
  else if(strcmp(command, "list") == 0)
  {
    if (argc != 4)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];

    return list_box(register_pipe_name, pipe_name);
  }
  else
  {
    print_usage();
    return -1;
  }

  return 0;
}
