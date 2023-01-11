#ifndef __UTILS_CLIENT_H__
#define __UTILS_CLIENT_H__

#include "wire_protocol.h"

int connect(OP_CODE_SIZE op_code, char *register_pipe_name, char *client_pipe_name, char *box_name);

#endif