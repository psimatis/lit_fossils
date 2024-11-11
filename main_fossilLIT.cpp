// TODO: Currently T is constant. Should it move? How?

#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/hint_m.h"
#include "./indices/live_index.cpp"
#include "./indices/fossil_index.h"


void usage(){
    cerr << endl << "PROJECT" << endl;
    cerr << "       LIT with Fossil Index for Old Intervals" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_fossilLIT.exec [OPTIONS] [STREAMFILE]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -e" << endl;
    cerr << "              set the leaf partition extent; it is set in seconds" << endl;
    cerr << "       -b" << endl;
    cerr << "              set the type of data structure for the LIVE INDEX" << endl;
    cerr << "       -c" << endl;
    cerr << "              set the capacity constraint number for the LIVE INDEX" << endl; 
    cerr << "       -d" << endl;
    cerr << "              set the duration constraint number for the LIVE INDEX" << endl;      
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "       -T" << endl;
    cerr << "              set the fossil threshold; intervals with end time <= T are added to the Fossil Index" << endl << endl;
    cerr << "EXAMPLE" << endl;
    cerr << "       ./query_fossilLIT.exec -e 86400 -b ENHANCEDHASHMAP -c 10000 -T 200000 streams/BOOKS.mix" << endl << endl;
}

int main(int argc, char **argv){
    Timer tim;
    HINT_M_Dynamic *idxR;
    LiveIndex *lidxR;
    RunSettings settings;

    size_t maxCapacity = -1, maxNumBuffers = 0;
    size_t totalResult = 0, queryresult = 0, numQueries = 0, numUpdates = 0;

    Timestamp first, second, startEndpoint, leafPartitionExtent = 0, maxDuration = -1;

    double totalIndexTime = 0, totalBufferStartTime = 0, totalBufferEndTime = 0, totalIndexEndTime = 0, totalQueryTime_b = 0, totalQueryTime_i = 0;
    double vm = 0, rss = 0, vmMax = 0, rssMax = 0;
    double unused1, unused2; // Dummy variables consuming the data stream
    
    char c, operation;
    string typeBuffer;

    // Fossil index
    FossilIndex fossilIndex;
    int T = 100000;
    double totalQueryTimeFossil = 0;
    
    settings.init();
    settings.method = "fossilLIT";
    while ((c = getopt(argc, argv, "q:e:c:d:b:r:T:")) != -1){
        switch (c){
            case '?':
                usage();
                return 0;
            case 'e':
                leafPartitionExtent = atoi(optarg);
                if (leafPartitionExtent <= 0){
                    usage();
                    return 1;
                }
                break;
            case 'b':
                typeBuffer = toUpperCase((char*)optarg);
                break;
            case 'c':
                maxCapacity = atoi(optarg);
                break;
            case 'd':
                maxDuration = atoi(optarg);
                break;
            case 'r':
                settings.numRuns = atoi(optarg);
                break;
            case 'T':
                T = atoi(optarg);
                break;
        }
    }
    
    if (argc - optind != 1){
        usage();
        return 1;
    }


    tim.start();
    idxR = new HINT_M_Dynamic(leafPartitionExtent);
    totalIndexTime = tim.stop(); // Question: this variable is never printed.

    if (maxCapacity != -1){
        if (typeBuffer == "MAP")
            lidxR = new LiveIndexCapacityConstraintedMap(maxCapacity);
        else if (typeBuffer == "VECTOR")
            lidxR = new LiveIndexCapacityConstraintedVector(maxCapacity);
        else if (typeBuffer == "ENHANCEDHASHMAP")
            lidxR = new LiveIndexCapacityConstraintedICDE16(maxCapacity);
        else{
            usage();
            return 1;
        }
    }
    else if (maxDuration != -1){
        if (typeBuffer == "MAP")
            lidxR = new LiveIndexDurationConstraintedMap(maxDuration);
        else if (typeBuffer == "VECTOR")
            lidxR = new LiveIndexDurationConstraintedVector(maxDuration);
        else if (typeBuffer == "ENHANCEDHASHMAP")
            lidxR = new LiveIndexDurationConstraintedICDE16(maxDuration);
        else{
            usage();
            return 1;
        }
    }
    else{
        usage();
        return 1;
    }

    // Load stream
    settings.queryFile = argv[optind];
    ifstream fQ(settings.queryFile);
    if (!fQ){
        usage();
        return 1;
    }

    int id;
    Timestamp startTime, endTime;
    while (fQ >> operation >> first >> second >> unused1 >> unused2){
    // Explanation:
    // Operation is S/E to start/end an interval, or Q to query.
    // Fist is the inteval ID if S/E, or query start time if Q.
    // Second is the start/end time if S/E, or query end time if Q.
    // unused1 is the end time but is not used
    // unused2 is only used by aLit. It is probably extra attribute to index.
        switch (operation){
            case 'S':
                id = first;
                startTime = second;
                tim.start();
                lidxR->insert(id, startTime);
                totalBufferStartTime += tim.stop();
                
                numUpdates++;
                
                process_mem_usage(vm, rss);
                vmMax = max(vm, vmMax);
                rssMax = max(rss, rssMax);
                break;

            case 'E':
                id = first;
                endTime = second;
                tim.start();
                startEndpoint = lidxR->remove(id); // This returns the start timestamp of the deleted interval
                totalBufferEndTime += tim.stop();
                
                tim.start();
                if (endTime <= T)
                    fossilIndex.insertInterval(id, startEndpoint, endTime); // Add to FossilIndex
                else
                    idxR->insert(Record(id, startEndpoint, endTime));
                totalIndexEndTime += tim.stop();

                numUpdates++;
                
                process_mem_usage(vm, rss);
                vmMax = max(vm, vmMax);
                rssMax = max(rss, rssMax);
                break;

            case 'Q':
                Timestamp qStart = first;
                Timestamp qEnd = second;
                numQueries++;

                for (auto r = 0; r < settings.numRuns; r++){
                    tim.start();
                    // Question: Why does the query takes numQueries and uses it as id?
                    queryresult = lidxR->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
                    totalQueryTime_b += tim.stop();
                    // Question: Is buffer time synonymous to live index time?

                    tim.start();
                    if (qStart <= idxR->gend){
// Question: Waht is workload count?
#ifdef WORKLOAD_COUNT
                        queryresult += idxR->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
#else
                        queryresult ^= idxR->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
#endif
                    }
                    totalQueryTime_i += tim.stop();

                    tim.start();
                    if (qStart <= T)
                        queryresult += fossilIndex.query(qStart, qEnd);
                    totalQueryTimeFossil += tim.stop();
                }
                totalResult += queryresult;
                
                process_mem_usage(vm, rss);
                vmMax = max(vm, vmMax);
                rssMax = max(rss, rssMax);
                break;
        }
        maxNumBuffers = max(maxNumBuffers, lidxR->getNumBuffers());
    }
    fQ.close();

    // Report
    idxR->getStats();
    cout << endl << "fossilLIT" << endl;
    cout << "====================" << endl << endl;
    cout << "Buffer info" << endl;
    cout << "Type                               : " << typeBuffer << endl;
    if (maxCapacity != -1)
        cout << "Buffer capacity                    : " << maxCapacity << endl;
    else
        cout << "Buffer duration                    : " << maxDuration << endl;
    cout << "T                                  : " << T << endl << endl;
    cout << "Index info" << endl;
    cout << "Updates report" << endl;
    cout << "Num of updates                     : " << numUpdates << endl;
    cout << "Num of buffers  (max)              : " << maxNumBuffers << endl;
    cout << "Total updating time (buffer) [secs]: " << (totalBufferStartTime + totalBufferEndTime) << endl;
    cout << "Total updating time (index)  [secs]: " << totalIndexEndTime << endl;
    cout << "Num of fossils                     : " << fossilIndex.getObjectCount() << endl << endl;

    cout << "Queries report" << endl;
    cout << "Num of queries                     : " << numQueries << endl;
    cout << "Num of runs per query              : " << settings.numRuns << endl;
    cout << "Total result [";
#ifdef WORKLOAD_COUNT
    cout << "COUNT]               : ";
#else
    cout << "XOR]                 : ";
#endif
    cout << totalResult << endl;
    cout << "Total querying time (buffer) [secs]: " << (totalQueryTime_b / settings.numRuns) << endl;
    cout << "Total querying time (index)  [secs]: " << (totalQueryTime_i / settings.numRuns) << endl;
    cout << "Total querying time (fossil) [secs]: " << (totalQueryTimeFossil / settings.numRuns) << endl;


    delete lidxR;
    delete idxR;
    
    return 0;
}
