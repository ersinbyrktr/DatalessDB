#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
//#define DEBUG 1
#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif
#include <sys/socket.h>	
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#define  BUFFER_SIZE 1024


/**************************************
          TYPEDEF SECTION
**************************************/

typedef struct sockaddr_in sockaddrin;
typedef struct sockaddr socketaddr;

typedef struct erso_row_struct {
	char** columns;
	char* raw;
	int row_id;
} erso_row;

typedef struct socket_comp_struct {
	int sockfd;
	int client_conn;
	int port;
	sockaddrin serv_addr;
	sockaddrin client_addr;
	socklen_t clilen;
} socket_comp;

typedef struct table_struct {
	int column_num;
	const int* column_type;
	//char** column_name;
	const char* name;
} generated_table;

typedef struct generate_obj_struct {
	socket_comp* socky;
	generated_table* table;
	//const char* scale_factor;
	//const char* datagen;
	erso_row* row;
	char buffer[BUFFER_SIZE];
	char *cursor;
	char cmd[300];
	int has_data;
	int buffer_size;
	int table_id;
	int initialized;
	int recv_data_size;
} generate_obj;

/*********************************
      FUNCTION DECLERATIONS
*********************************/
int setTable(generate_obj*);
void my_segfault_sigaction(int);
int set_config(generate_obj*);
int create_socket_connection(socket_comp*);
int accept_socket_connection(socket_comp*);
int start_myriad(generate_obj*);
int connect_generator(generate_obj*);
int fetch_new(generate_obj*);
int get_next_line(generate_obj*);
/*****************************
      END OF DECLERATIONS
******************************/

/**************************************************************/
/*-----------------------( CONFIG )---------------------------*/
/**************************************************************/
int set_config(generate_obj* my_generator) {
	my_generator->table = (generated_table*)malloc(sizeof(generated_table));
	if(setTable(my_generator) < 0){
		return -1;
	}
	srand(time(NULL));
	my_generator->buffer_size = BUFFER_SIZE;
	my_generator->has_data = 0;
	my_generator->initialized = 1;
	my_generator->cursor = my_generator->buffer;
	my_generator->socky = (socket_comp*) malloc(sizeof(socket_comp));
	my_generator->recv_data_size = 0;
	
	my_generator->socky->port = rand() % 100; //random number between 0 and 100
	my_generator->socky->port += 5221;

	sprintf(my_generator->cmd,
		"./generator %d &",
		my_generator->socky->port
	);
	printf("PORT: %d TABLE: %s \n", my_generator->socky->port, my_generator->table->name);

	my_generator->row = (erso_row*)malloc(sizeof(erso_row));
	my_generator->row->columns = (char**)malloc(sizeof(char *) * (my_generator->table->column_num));
	my_generator->row->raw = (char*)malloc(sizeof(char) * 200);
	my_generator->row->row_id = 0;
	return 0;
}


int setTable(generate_obj* my_generator) {
	int tmp1[9]  = {1,0,0,0,0,1,0,2,0};
	int tmp2[7]  = {1,0,0,1,0,2,0};
	int tmp3[5]  = {1,1,1,2,0};
	int tmp4[8]  = {1,0,0,1,0,2,0,0};
	int tmp5[4]  = {1,0,1,0};
	int tmp6[3]  = {1,0,0};
	//int tmp7[16] = {1,1,1,1,1,2,2,2,0,0,0,0,0,0,0,0};
	int tmp8[9]  = {1,1,0,1,0,0,1,0,1};

	switch (my_generator->table_id) {
		//case 2:
		case 6:
			my_generator->table->name = "part";
			my_generator->table->column_num = 9;	
			my_generator->table->column_type = tmp1;
			return 0;
		case 600:
			my_generator->table->name = "supplier";
			my_generator->table->column_num = 7;	
			my_generator->table->column_type = tmp2;
			return 0;
		case 7:
			my_generator->table->name = "part_supp";
			my_generator->table->column_num = 5;		
			my_generator->table->column_type = tmp3;
			return 0;
		case 5:
			my_generator->table->name = "customer";
			my_generator->table->column_num = 8;		
			my_generator->table->column_type = tmp4;
			return 0;
		case 3:
			my_generator->table->name = "nation";
			my_generator->table->column_num = 4;		
			my_generator->table->column_type = tmp5;
			return 0;
		case 4:
			my_generator->table->name = "region";
			my_generator->table->column_num = 3;		
			my_generator->table->column_type = tmp6;
			return 0;
		case 10:
		case 11:
			my_generator->table->name = "lineitem";
			my_generator->table->column_num = 9;		
			my_generator->table->column_type = tmp8;
			return 0;
		case 9:
			my_generator->table->name = "order";
			my_generator->table->column_num = 9;		
			my_generator->table->column_type = tmp8;
			return 0;
		default:
			return -1;
	}
}


int create_socket_connection(socket_comp* my_socket) {
	/********* SET SOCKET PARAMETERS *********/
	my_socket->sockfd = socket(AF_INET, SOCK_STREAM, 0);
	memset(&(my_socket->serv_addr), 0, sizeof(sockaddrin));
	my_socket->serv_addr.sin_family = AF_INET;
	my_socket->serv_addr.sin_addr.s_addr = INADDR_ANY;
	my_socket->serv_addr.sin_port = htons(my_socket->port);

	/********* BIND CONNECTION AND LISTEN FOR INCOMMING CONNECTIONS *********/
	if (bind(my_socket->sockfd, (const socketaddr*)&my_socket->serv_addr, sizeof(sockaddrin)) < 0) {
		printf("\nERROR IN SOCKET BIND\n");
		return -1;
	}
	if (listen(my_socket->sockfd, 5) < 0) {
		printf("\nERROR IN SOCKET LISTEN\n");
		return -1;
	}
	return 0;
}

int accept_socket_connection(socket_comp* my_socket) {
	/********* ACCEPT SOCKET CONNECTIONS *********/
	my_socket->client_conn = accept(my_socket->sockfd, (socketaddr*)&(my_socket->client_addr), &(my_socket->clilen));
	printf("\nConnection accepted\n");
	if (my_socket->client_conn < 0) {
		printf("ERROR on accept");
		return -1;
	}
	return 0;
}

void destroy_gen(generate_obj* my_generator) {
	close(my_generator->socky->sockfd);
	free(my_generator->socky);
	/*for (i = 0; i < my_generator->table->column_num; i++) {
		free(my_generator->row->columns[i]);
	}*/
	free(my_generator->row->columns);
	free(my_generator->row);
	free(my_generator->row->raw);
}

int start_myriad(generate_obj* my_generator) {
	printf("cmd executed: %s\n", my_generator->cmd);
	return system(my_generator->cmd);
}

int connect_generator(generate_obj* my_generator) {
	if (create_socket_connection(my_generator->socky) < 0) return -1;

	/********* INVOKE EXECUTION OF COMMAND TO GENERATE THE DATA *********/
	if (start_myriad(my_generator) < 0) return -1;

	/********* ACCEPT INCOMMING CONNECTION *********/
	if (accept_socket_connection(my_generator->socky) < 0) return -1;
	//memset(my_generator->buffer, 0, BUFFER_SIZE);	
	return fetch_new(my_generator);
}

int fetch_new(generate_obj* my_generator) {
	int recv_length=1234567;
	memset(my_generator->buffer, 0, BUFFER_SIZE);
	if ((recv_length = recv(my_generator->socky->client_conn, my_generator->buffer, my_generator->buffer_size - 1, 0)) < 1) {
		return -1;
	}
	DEBUG_PRINT(("requested:%d, received:%d, received2:%d  \n", my_generator->buffer_size - 1, my_generator->recv_data_size, recv_length));
	printf("received:%d \n", (int)strlen(my_generator->buffer));
	my_generator->cursor = my_generator->buffer;
	my_generator->has_data = 1;
	return 0;
}

int get_line(generate_obj* my_generator) {
	char* column,*tmp_row = my_generator->row->raw;
	int i = 0;
	if (get_next_line(my_generator) < 0) {
		return -1;
	}
	//printf("raw1:%s \n", my_generator->row->raw);
	while ((column = strtok_r(tmp_row, "|", &(tmp_row)))) {
	//printf("raw:%s \n", my_generator->row->raw);
		//memset(my_generator->row->columns[i],0,200);
		my_generator->row->columns[i] = column;
		//strcpy(my_generator->row->columns[i], column);
		i++;
	}
	//printf("raw2:%s \n", my_generator->row->raw);
	return 0;
}


int get_next_line(generate_obj* my_generator) {
	char *line;
	int i;
	memset(my_generator->row->raw, 0, 200);
	if (!my_generator->has_data) {
		DEBUG_PRINT(("No Data Found in the buffer. Will be fetched..\n"));
		if (fetch_new(my_generator) < 0) {
			DEBUG_PRINT(("Couldn't fetch data\n"));
			return -1;
		}
	}
	/* Check if it is the last line in the buffer */
	if (!strstr(my_generator->cursor, "\n")) {
		strcpy(my_generator->row->raw, my_generator->cursor);
		DEBUG_PRINT(("last_line: "));
		for (i = 0; i < strlen(my_generator->row->raw); i++) DEBUG_PRINT(("%u,", (unsigned int)my_generator->row->raw[i]));
		DEBUG_PRINT(("\n"));
		my_generator->has_data = 0;
		if (fetch_new(my_generator) < 0) {
			DEBUG_PRINT(("Couldn't fetch data\n"));
			return -1;
		}
		if (my_generator->cursor[0] == '\n') {
			my_generator->cursor++;
			my_generator->row->row_id++;
			return 0;
		}
		if ((line = strtok_r(my_generator->cursor, "\n", &my_generator->cursor))) {
			strcat(my_generator->row->raw, line);
			my_generator->row->row_id++;
			return 0;
		}
		else {
			DEBUG_PRINT(("weird\n"));
			return -1;
		}
	}

	if ((line = strtok_r(my_generator->cursor, "\n", &my_generator->cursor))) {
		strcpy(my_generator->row->raw, line);
		my_generator->row->row_id++;
		return 0;
	}
	else {
		DEBUG_PRINT(("weird2"));
		my_generator->has_data = 0;
		if (fetch_new(my_generator) < 0) {
			DEBUG_PRINT(("Couldn't fetch data\n"));
			return -1;
		}
		if ((line = strtok_r(my_generator->cursor, "\n", &my_generator->cursor))) { 
			my_generator->row->row_id++;
			DEBUG_PRINT(("line:%s\n", line));
		}
	}
	return 0;
}
