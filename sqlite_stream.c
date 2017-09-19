#include "sqlite_stream.h"

void my_segfault_sigaction(int signal){
    printf("\nSEGFAULT\n");
    exit(0);
}

int main(int argc, char* argv[]) {
	int i;
    signal(SIGSEGV, my_segfault_sigaction);
	generate_obj my_generator;
	if(argc==1)
		my_generator.table_id = 6;
	else
		my_generator.table_id = atoi(argv[1]);
	if(set_config(&my_generator)<0)
		return 0;
	connect_generator(&my_generator);
	if (argc <= 2) {
		get_line(&my_generator);
		printf("row_id:%d  c1:%s c2:%s c3:%s c4:%s c5:%s c6:%s c7:%s c8:%s c9:%s\n", 
			my_generator.row->row_id, my_generator.row->columns[0], my_generator.row->columns[1], my_generator.row->columns[2], 
			my_generator.row->columns[3], my_generator.row->columns[4], my_generator.row->columns[5], my_generator.row->columns[6], my_generator.row->columns[7], my_generator.row->columns[8]);
	}
	else {
		int count = atoi(argv[2]);
		for (i=0; i < count; i++) {
			get_line(&my_generator);
			/*printf("row_id:%d  c1:%s c2:%s c3:%s c4:%s c5:%s c6:%s c7:%s c8:%s c9:%s\n", 
				my_generator.row->row_id, my_generator.row->columns[0], my_generator.row->columns[1], my_generator.row->columns[2], 
				my_generator.row->columns[3], my_generator.row->columns[4], my_generator.row->columns[5], my_generator.row->columns[6], my_generator.row->columns[7], my_generator.row->columns[8]);*/
		}
	}
	destroy_gen(&my_generator);

	system("pkill tpch-gen");
}