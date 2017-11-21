/****************** CLIENT CODE ****************/

#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
	int clientSocket, port, i, n;
	int len, length;
	srand(time(0));

	char buf[1024] = "57216|116492|F|445710.94|1993-07-30|2-HIGH|Clerk#000000867|0| final foxes across the enticingly final deposits eat furi|\n"
		"57217|68980|F|199128.06|1995-01-22|3-MEDIUM|Clerk#000000209|0|lithely pending reque|\n"
		"57218|127022|O|93892.81|1997-06-12|5-LOW|Clerk#000000700|0| dependencies wake slyly. blithely regular dependencies alo|\n"
		"57219|57973|O|157096.04|1995-11-02|1-URGENT|Clerk#000000603|0|wake around the furiously r|\n"
		"57220|99793|O|110315.52|1995-09-04|1-URGENT|Clerk#000000646|0|ding excuses. carefully final instructions use quickly final r|\n\0\0";

	char *buffer;
	struct sockaddr_in serverAddr;
	socklen_t addr_size;

	port = atoi(argv[1]);
	/*---- Create the socket. The three arguments are: ----*/
	/* 1) Internet domain 2) Stream socket 3) Default protocol (TCP in this case) */
	clientSocket = socket(AF_INET, SOCK_STREAM, 0);

	/*---- Configure settings of the server address struct ----*/
	/* Address family = Internet */
	serverAddr.sin_family = AF_INET;
	/* Set port number, using htons function to use proper byte order */
	serverAddr.sin_port = htons(port);
	/* Set IP address to localhost */
	serverAddr.sin_addr.s_addr = INADDR_ANY;
	/* Set all bits of the padding field to 0 */
	memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

	/*---- Connect the socket to the server using the address struct ----*/
	addr_size = sizeof serverAddr;
	connect(clientSocket, (struct sockaddr *) &serverAddr, addr_size);

	length = strlen(buf);
	printf("buf_length  : %d", length);
	for (i = 0; i < 300000; i++) {
		buffer = buf;
		len = length;
		while (len > 0) {
			n = write(clientSocket, buffer, len);
			if (n < 0) {
				printf("[ERROR] Unable to send reponse.\n");
				printf("Oh dear, something went wrong with write()! %s\n", strerror(errno));
				close(clientSocket);
				return -1;
			}
			else
				if (n == 0) { // connection closed!
					printf("[ERROR] Connection has been unexpectedly closed by remote side!\n");
					close(clientSocket);
					return -2;
				}
				else {
					len -= n;
					buffer += n;
					//printf("Response sent (%d bytes).\n", n);
				}
		}
	}


	return 0;
}