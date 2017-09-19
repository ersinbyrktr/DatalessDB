#include "sqlite_stream2.h"

void my_segfault_sigaction(int signal){
    printf("\nSEGFAULT\n");
    exit(0);
}

int main(int argc, char* argv[]) {
	int i, k=0;
    signal(SIGSEGV, my_segfault_sigaction);
	generate_obj my_generator;
	if(argc==1)
		my_generator.table_id = 11;
	else if(argc==2)
		my_generator.table_id = atoi(argv[1]);
	else{
		my_generator.table_id = atoi(argv[1]);
		k=1;
	}
	if(set_config(&my_generator)<0)
		return 0;
	connect_generator(&my_generator, k);

	destroy_gen(&my_generator);

	//system("pkill tpch-gen");
}