#include <fcntl.h> 
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <aio.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>

#define FILE_SIZE 10485760   //In 'Byte'

int file_size;
int buffer_sizes[] = {0,0,0,0,0,0,0,0,0,0};  //Max '10' Thread
int offsets[]      = {0,0,0,0,0,0,0,0,0,0};


struct thread_arg{    //Thread Arguments
    int thread_id;    //which thread
    int source_file;  //source file
    int dest_file;    //destination file
    int offset;       //offset of the thread
    int buffer_size;  //buffer size of the thread
};

void create_file(int file_size, int thread_number,char *source_path){
  //Fills the source file
  
  FILE *file;
  char *buffer = malloc((file_size+1)*sizeof(char));

  int char_value = 65;			  //description of character
  int thread_piece = 0; 		  //size of all threads
  int remainder = 0;      		  //remaining part from division

  remainder = file_size%thread_number;
  thread_piece = (file_size - remainder)/thread_number;

printf("[Source File Creation is started]\n");
  
  //Fills the buffers of all threads with base size.
  int i = 0; 
  for ( i = 0; i < thread_number; i++) {
      buffer_sizes[i] = thread_piece;
  }

  //Add the remainder sizes to last thread.
  buffer_sizes[thread_number-1] +=remainder;

  // First threads offset is 0
  offsets[0] = 0;

  //Fill the other thread offsets.
  for (i = 1; i < thread_number; i++) {
      offsets[i] = offsets[i-1] + buffer_sizes[i-1];
  }

  int j, file_counter = 0;
  srand ( (unsigned)time(NULL)); //rand()-to generate different numbers
  
  //Fills the buffer for writing to the source file
  for (i = 0; i < thread_number && file_counter <file_size; i++) {
      for (j = 0; j < buffer_sizes[i]; j++) {
          buffer[file_counter] = (char)char_value;
          file_counter++;
	      
		  //Changes the char_value according random value.
		  do{
			char_value=rand() % (122 + 1 - 65) + 65;//(A-Z)(a-z)
		  }while(char_value>90 && char_value<97);	//for alphabetic characters
      }

  }


  buffer[file_size] = '\0'; //balances the file
  file = fopen(source_path,"w");

  fprintf(file, buffer);
  fclose(file);
  printf("\n[Source File Creation Completed]\n");

}

void* asynch_copy(void* arguments){
   
    struct thread_arg *args = (struct thread_arg*) arguments;
	struct aiocb aio;

    int err, ret;
	int dest_file   = args->dest_file ;	int source_file = args->source_file;
	int thread_id = args->thread_id;	int offset = args->offset;
	int buffer_size_total=args->buffer_size;
	char* buffer;  
	
    printf("\n[%d].Thread ----> CREATED  \n",thread_id);
	
    ////Asynch,Partial Read And Write////
	int buffer_size=0;
	int balance=buffer_size_total;
	srand ( (unsigned)time(NULL));
	
	while(balance > 0){                            //Creates a real scenario by reading and writing different sizes in each loop.
		                                           //Written random formula just is my opinion.Therefore, it can be changed.
	if((balance	   <= buffer_size_total/9)
				   ||(buffer_size_total<10)){   //controls the last state to block the infinite loop.
		buffer_size	= balance;
		balance	    = 0;
	}
	else{
		do{                                        //takes random value for reading and writing values
		buffer_size	= rand() % ((int)(buffer_size_total/4) + 1 - (int)(buffer_size_total/10)) + (int)(buffer_size_total/10);
		}while(buffer_size > balance);
		balance   -= 	buffer_size;
	}
	
    buffer = malloc(buffer_size*sizeof(char));    //Routine coding sections for reading and writing
    memset(&aio, 0, sizeof(struct aiocb));
    memset(buffer, 0 ,sizeof(buffer));

    aio.aio_fildes = source_file;
    aio.aio_buf    = buffer;
    aio.aio_offset = offset;
    aio.aio_nbytes = buffer_size;

    aio_read(&aio);

    while(aio_error(&aio) == EINPROGRESS){}

    err = aio_error(&aio);
    ret = aio_return(&aio);

    if(err != 0)
    {
         printf("\nError at aio_error():%s\n", strerror(err));
         close(source_file);
         exit(2);
    }

    aio.aio_fildes = dest_file;

    aio_write(&aio);  //Copy operation

    
    while(aio_error(&aio) == EINPROGRESS){}

    err = aio_error(&aio);

    if(err != 0)
    {
         printf("Error at aio_error():%d\n", err);
         close(dest_file);
         exit(2);
    }

    ret = aio_return(&aio);
	
	offset+=buffer_size; //add new value to offset 
	free(buffer);        //sets free the malloc
	                     //Writes state to terminal
	printf("\n[%d].Thread:[Progress: --->%%%ld<---] (Copy operation continues at Offset: %d)   \n",thread_id, 100-(balance*100/buffer_size_total),offset);
	
	}
	////-----------------////

    
    printf("\n[%d].Thread: completed the operation.\n",thread_id );

    pthread_exit(NULL);

}

int main(int argc, char* argv[]){
  int thread_count = 0;
  int file_size; 
  char* dest_path = malloc(200*sizeof(char));
  char* source_path = malloc(200*sizeof(char));

  char *source_filename = "source.txt";
  char *dest_filename   = "destination.txt";
  
  ////Takes inputs////
  if(strcmp(argv[1],"-")!=0){
      strcpy(source_path, argv[1]);
      strcat(source_path, source_filename);
  }else{
      strcpy(source_path,source_filename);

  }

  if(strcmp(argv[2],"-")!=0){
      strcpy(dest_path, argv[2]);
      strcat(dest_path, dest_filename);
  }else{
      strcpy(dest_path,dest_filename);
  }
  
  thread_count = atoi(argv[3]);

  
  file_size    = FILE_SIZE;
  if(file_size > 104857600){
	printf("\nEnter Valid File Size!(File Size must be smaller or equal than 104857600Byte(100MB))\n");
    return 0;
  }
  
  pthread_t threads[thread_count];
  struct thread_arg args[thread_count];
  
  
  printf("\n---Parallel File Copy (Async I/O)---\n");
  printf("\nThread number:   %d\n",thread_count);
  printf("File size(Byte): %d",file_size);
  printf("     (MegaByte):  %d\n\n",file_size/1048576);
  printf("--------------------------------------\n");
  
  ////Create source File////
  create_file(file_size,thread_count,source_path); 

  mode_t file_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  int source_file = 0, dest_file = 0;
  dest_file   = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, file_mode);
  source_file = open(source_path, O_RDONLY);
  
  if(source_file == -1)
  {
      perror("source error");
      return EXIT_FAILURE;
  }
  if(dest_file == -1)
  {
      perror("dest error");
      return EXIT_FAILURE;
  }


    int i = 0;
    for (i = 0; i < thread_count; i++) {

        args[i].thread_id = i+1;
        args[i].source_file    = source_file;
        args[i].dest_file      = dest_file;
        args[i].offset    = offsets[i];
        args[i].buffer_size  = buffer_sizes[i];
    }

    ////CREATING THREAD////
      int position = 0;

      for (i = 0; i < thread_count; i++) {
		 position = pthread_create(&threads[i], NULL, asynch_copy, (void*)&(args[i]));
		 if(position != 0){ printf("Thread creation error\n");}
      }

      for (i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
      }

	  close(dest_file);
      close(source_file);
      
printf("\n[File copy operation is finished.]\n" );
      return 0;
}
