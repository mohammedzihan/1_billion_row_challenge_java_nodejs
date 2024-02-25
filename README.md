1 Billionr Row Challenge: 

![Image](1brc.png)


Task: Parsing temperature measurement values from a text file and determining the min, max, and mean temperature for each weather station present. The caveat: the file has 1,000,000,000 entries!

The challenge presented opportunity to explore some high-performance programming techniques like - Vector API which leverages CPU SIMD instructions, parallelizing the computation, speeding up the application start-up, tuning the garbage collector, and much more. 

My overall plan was to partition the file into ranges equal to the number of cores available on the target processor for parallelization. For each partition, create a worker task that computes statistics for each weather station on separate threads. When these tasks are finished, the final results are aggregated into a final table of statistics. Tried out with 100 Million instead of 1 Billion entries. 

In NodeJS, used Worker threads to process each partitioned file & leverage multiple cores, File streams & Generators to read & process the file chunks & avoid memory overhead, Bit shifting & Buffers to efficiently parse & aggregate the station names & temperatures. The overall system time (System config: 16GB RAM, 8vCPU) for processing 100 Million rows came out 5.43 seconds with 731% CPU utilization, 125784 Max RSS. 

O/P --> 41.38user 0.78system 0:05.76elapsed 731%CPU (0avgtext+0avgdata 125784maxresident)k
62848inputs+0outputs (542major+25530minor)pagefaults 0swaps

In Java, experimented with a few different approaches using parallel streams, completable futures, executor threadpool, treemap for sorted aggregation, unsafe memory allocation for direct access to memory. Best system elapsed time achieved in Java came out to be 3.80 seconds for processing 100 Million entries. 

O/P --> 14.06user 1.68system 0:03.80elapsed 414%CPU (0avgtext+0avgdata 4558884maxresident)k
0inputs+88outputs (11major+822216minor)pagefaults 0swaps

TODO: 
1. Trying out different file reading strategy like using a memory-mapped file if the entire file can fit into memory.
2. Custom hashmap with 2-byte hashes instead of using built in Map
3. Flamegraph
