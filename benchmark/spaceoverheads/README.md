### Instructions

#### modify the spaceoverheads.sh

total file size = SIZE * N

DIRECTORY="/data/HDD_Sdd_data/spaceoverheads_1K"	//write directory

SIZE="1K"			//the size of a single file

BS="1K"				//fio block size. must larger than SIZE

NUMJOBS=512	//the number of tasks for a single fio process

N=1048576 		//numer of file

DISK="sdd" 		//disk monitered by iostat
