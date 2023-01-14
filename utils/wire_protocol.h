// read/write lib
#include <unistd.h>

// fifos lib
#include <sys/stat.h>

// O_RDONLY and O_WRONLY lib
#include <fcntl.h>

// uint_8 lib
#include <stdint.h>

// sprintf and sscanf
#include <stdio.h>

#include "string.h"

// OP_CODES
// TODO: change to be more descriptive
#define REGISTER_PUBLISHER 1
#define REGISTER_SUBSCRIBER 2
#define CREATE_BOX 3
#define RETURN_CREATE_BOX 4
#define DELETE_BOX 5
#define RETURN_DELETE_BOX 6
#define LIST_BOXES 7
#define RETURN_LIST_BOXES 8
#define SEND_MESSAGE 9
#define SEND_SUBSCRIBER 10

// SIZES
#define PROTOCOL_MESSAGE_SIZE 1064

#define OP_CODE_SIZE uint8_t
#define RETURN_CODE_SIZE uint32_t
#define PIPE_NAME_SIZE 256
#define BOX_NAME_SIZE 32
#define MESSAGE_SIZE 1024
#define BOX_SIZE uint64_t