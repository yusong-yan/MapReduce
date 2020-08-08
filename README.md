# MapReduce_System_Implementation
### In this distributed system, a worker process that calls application Map and Reduce functions and handles reading and writing files,and a master process that hands out tasks to workers and copes with failed workers. It made to dealing with large data by using multiple computers.
### Link to MapReduce Paper https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf
##### In this example, 3 computer start worker process and send message through rpc to the master machine
<img width="300" alt="Screen Shot 2020-08-07 at 5 21 07 PM" src="https://user-images.githubusercontent.com/46516278/89698430-02374900-d8d6-11ea-84a5-fce392d21435.png">
<img width="450" alt="Screen Shot 2020-08-07 at 5 21 22 PM" src="https://user-images.githubusercontent.com/46516278/89698453-285ce900-d8d6-11ea-960b-94db324a0995.png">
<img width="450" alt="Screen Shot 2020-08-07 at 5 21 31 PM" src="https://user-images.githubusercontent.com/46516278/89698455-298e1600-d8d6-11ea-8e94-dfeee4063aaa.png">
<img width="450" alt="Screen Shot 2020-08-07 at 5 21 41 PM" src="https://user-images.githubusercontent.com/46516278/89698473-46c2e480-d8d6-11ea-8c23-a0155d346ec8.png">
<img width="1234" alt="Screen Shot 2020-08-07 at 6 34 24 PM" src="https://user-images.githubusercontent.com/46516278/89699562-ac19d400-d8dc-11ea-9c36-5c4493d44ca5.png">

### To run it, go to main folder 

#### Start Master Server
go build -buildmode=plugin ../mrapps/wc.go


go run mrmaster.go pg-*.txt

#### Start worker (as many processes as you want)
 go run mrworker.go wc.so
 
#### Check output
cat mr-out-* | sort | more
