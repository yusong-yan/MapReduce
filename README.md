# MapReduce_System_Implementation
### In this fault tolerant distributed system, a master server hands out tasks to workers and copes with failed workers, and worker processes will calls application Map and Reduce functions and handles reading and writing files. 
#### Master : src/mr/master.go
##### Link to MapReduce Paper https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf
##### In this example, 3 computer start worker process and send message through rpc to the master machine
<img width="500" alt="Screen Shot 2020-08-07 at 5 21 07 PM" src="https://user-images.githubusercontent.com/46516278/89698430-02374900-d8d6-11ea-84a5-fce392d21435.png">
##### Then master server allocated tasks to workers for Map and Reduce
<img width="650" alt="Screen Shot 2020-08-07 at 5 21 22 PM" src="https://user-images.githubusercontent.com/46516278/89698453-285ce900-d8d6-11ea-960b-94db324a0995.png">
<img width="650" alt="Screen Shot 2020-08-07 at 5 21 31 PM" src="https://user-images.githubusercontent.com/46516278/89698455-298e1600-d8d6-11ea-8e94-dfeee4063aaa.png">
<img width="650" alt="Screen Shot 2020-08-07 at 5 21 41 PM" src="https://user-images.githubusercontent.com/46516278/89698473-46c2e480-d8d6-11ea-8c23-a0155d346ec8.png">
##### One of the reduced file. There are 9 reduced files, and 63 intermediate files in this example
<img width="1234" alt="Screen Shot 2020-08-07 at 6 34 24 PM" src="https://user-images.githubusercontent.com/46516278/89699562-ac19d400-d8dc-11ea-9c36-5c4493d44ca5.png">

### To run it, go to main folder 

#### Start Master Server
go build -buildmode=plugin ../mrapps/wc.go


go run mrmaster.go pg-*.txt

#### Start worker (as many processes as you want)
 go run mrworker.go wc.so
 
#### Check output
cat mr-out-* | sort | more
