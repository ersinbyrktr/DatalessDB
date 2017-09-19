#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#define DEBUG 1
#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif
#include <sys/socket.h>	
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#define  BUFFER_SIZE 1024

/**************************************
          TYPE SECTION
**************************************/

typedef struct sockaddr_in sockaddrin;
typedef struct sockaddr socketaddr;


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
	const char* scale_factor;
	const char* datagen;
	char buffer[BUFFER_SIZE];
	char *cursor;
	char cmd[300];
	int has_data;
	int buffer_size;
	int table_id;
	int initialized;
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
int connect_generator(generate_obj*, int);
int fetch_new(generate_obj*, int);
int get_next_line(generate_obj*);
/*****************************
      END OF DECLERATIONS
******************************/

/**************************************************************/
/*-----------------------( CONFIG )---------------------------*/
/**************************************************************/
int set_config(generate_obj* my_generator) {
	int i;
	my_generator->table = (generated_table*)malloc(sizeof(generated_table));
	if(setTable(my_generator) < 0){
		return -1;
	}
	//printf("Table found: %s\n",my_generator->table_name);
	time_t t;
	srand((unsigned)time(&t));
	my_generator->buffer_size = BUFFER_SIZE;
	my_generator->has_data = 0;
	my_generator->initialized = 1;
	my_generator->cursor = my_generator->buffer;
	my_generator->datagen = getenv("DIANA_DATAGEN");
	my_generator->socky = (socket_comp*) malloc(sizeof(socket_comp));
	my_generator->scale_factor = getenv("SCALE_FACTOR");
	
	my_generator->socky->port = rand() % 100; //random number between 0 and 100
	my_generator->socky->port += 5221;
	sprintf(my_generator->cmd,
		"%stpch-gen-node %s -i0 -N1 -tsocket[%d] -Hlocalhost -x%s &",
		getenv("DIANA_DATAGEN"),
		my_generator->scale_factor,
		my_generator->socky->port,
		my_generator->table->name
	);
	printf("PORT: %d TABLE: %s \n", my_generator->socky->port, my_generator->table->name);

	return 0;
}


int setTable(generate_obj* my_generator) {
	int tmp1[9]  = {1,0,0,0,0,1,0,2,0};
	int tmp2[7]  = {1,0,0,1,0,2,0};
	int tmp3[5]  = {1,1,1,2,0};
	int tmp4[8]  = {1,0,0,1,0,2,0,0};
	int tmp5[4]  = {1,0,1,0};
	int tmp6[3]  = {1,0,0};
	int tmp7[16] = {1,1,1,1,1,2,2,2,0,0,0,0,0,0,0,0};
	int tmp8[11]  = {1,1,0,1,0,0,1,0,1,0,1};

	switch (my_generator->table_id) {
		//case 2:
		case 6:
			my_generator->table->name = "part";
			my_generator->table->column_num = 9;	
			my_generator->table->column_type = tmp1;
			return 0;
		case 3:
			my_generator->table->name = "supplier";
			my_generator->table->column_num = 7;	
			my_generator->table->column_type = tmp2;
			return 0;
		case 4:
			my_generator->table->name = "part_supp";
			my_generator->table->column_num = 5;		
			my_generator->table->column_type = tmp3;
			return 0;
		case 5:
			my_generator->table->name = "customer";
			my_generator->table->column_num = 8;		
			my_generator->table->column_type = tmp4;
			return 0;
		case 600:
			my_generator->table->name = "nation";
			my_generator->table->column_num = 4;		
			my_generator->table->column_type = tmp5;
			return 0;
		case 7:
			my_generator->table->name = "region";
			my_generator->table->column_num = 3;		
			my_generator->table->column_type = tmp6;
			return 0;
		case 8:
			my_generator->table->name = "lineitem";
			my_generator->table->column_num = 16;		
			my_generator->table->column_type = tmp7;
			return 0;
		case 9:
		case 11:
			my_generator->table->name = "order";
			my_generator->table->column_num = 11;		
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
	int i;
	close(my_generator->socky->sockfd);
	free(my_generator->socky);
}

int start_myriad(generate_obj* my_generator) {
	printf("cmd executed: %s\n", my_generator->cmd);
	return system(my_generator->cmd);
}

int connect_generator(generate_obj* my_generator, int j) {
	int i=0;
	if (create_socket_connection(my_generator->socky) < 0) return -1;

	/********* INVOKE EXECUTION OF COMMAND TO GENERATE THE DATA *********/
	if (start_myriad(my_generator) < 0) return -1;

	/********* ACCEPT INCOMMING CONNECTION *********/
	if (accept_socket_connection(my_generator->socky) < 0) return -1;
	while(i>-1){
		i = fetch_new(my_generator, j);
	}
	return 0;
}

int fetch_new(generate_obj* my_generator, int j) {
	int recv_length;
	memset(my_generator->buffer, 0, BUFFER_SIZE);
	if (recv_length = read(my_generator->socky->client_conn, my_generator->buffer, my_generator->buffer_size - 1) < 1) {
		return -1;
	}
	//DEBUG_PRINT(("requested:%d, received:%d \n", my_generator->buffer_size - 1, (int)strlen(my_generator->buffer)));
	if (j){
		DEBUG_PRINT(("%s \n", my_generator->buffer));		
	}
	my_generator->cursor = my_generator->buffer;
	my_generator->has_data = 1;
	return 0;
}
