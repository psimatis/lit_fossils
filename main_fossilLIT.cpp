// Notes 
// Tf moves in the middle of the latest end and its previous value if a memory threshold is reached.
// The live index is tiny (~0.002MB) compared to dead index (~50MB). 
// The current queries in Books.mix never touch the fossil index. I added a few in the end to query the fossils. How is the file created?

#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/live_index.cpp"
#include "./indices/fossil_index.h"
#include "./indices/hint_m_reconstructable.h"

using namespace std;

// Display instructions
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
    cerr << "EXAMPLE" << endl;
    cerr << "       ./query_fossilLIT.exec -e 86400 -b ENHANCEDHASHMAP -c 10000 streams/BOOKS.mix" << endl << endl;
}

// Parse arguments
void parseArguments(int argc, char** argv, RunSettings& settings, Timestamp& leafPartitionExtent, 
                    string& typeBuffer, size_t& maxCapacity, Timestamp& maxDuration, string& queryFile) {
    char c;
    settings.init();
    settings.method = "fossilLIT";

    while ((c = getopt(argc, argv, "q:e:c:d:b:r:")) != -1) {
        switch (c) {
            case 'e':
                leafPartitionExtent = atoi(optarg);
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
            case '?':
            default:
                throw invalid_argument("Invalid argument or option.");
        }
    }

    if (argc - optind != 1 || leafPartitionExtent <= 0) 
        throw invalid_argument("Invalid number of arguments. A stream file is required.");

    queryFile = argv[optind];
}

// Creates Live Index
LiveIndex* createLiveIndex(const string& typeBuffer, size_t maxCapacity, Timestamp maxDuration) {
    if (maxCapacity != -1) {
        if (typeBuffer == "MAP") return new LiveIndexCapacityConstraintedMap(maxCapacity);
        if (typeBuffer == "VECTOR") return new LiveIndexCapacityConstraintedVector(maxCapacity);
        if (typeBuffer == "ENHANCEDHASHMAP") return new LiveIndexCapacityConstraintedICDE16(maxCapacity);
    } 
    else if (maxDuration != -1) {
        if (typeBuffer == "MAP") return new LiveIndexDurationConstraintedMap(maxDuration);
        if (typeBuffer == "VECTOR") return new LiveIndexDurationConstraintedVector(maxDuration);
        if (typeBuffer == "ENHANCEDHASHMAP") return new LiveIndexDurationConstraintedICDE16(maxDuration);
    }
    throw invalid_argument("Invalid buffer type or constraints for Live Index.");
}

void displayMemoryUsage(LiveIndex* liveIndex, HINT_Reconstructable* deadIndex, FossilIndex fossilIndex) {
    size_t liveIndexSize = liveIndex->getMemoryUsage();
    size_t deadIndexSize = deadIndex->getMemoryUsage();
    size_t fossilIndexSize = fossilIndex.getDiskUsage();

    double liveIndexSizeMB = liveIndexSize / (1024.0 * 1024.0);
    double deadIndexSizeMB = deadIndexSize / (1024.0 * 1024.0);
    double fossilIndexSizeMB = fossilIndexSize / (1024.0 * 1024.0);

    cout << "\nSize Report:" << endl;
    cout << "Live Index Memory Usage            : " << liveIndexSizeMB << " MB" << endl;
    cout << "Dead Index Memory Usage            : " << deadIndexSizeMB << " MB" << endl;
    cout << "Fossil Index Disk Usage            : " << fossilIndexSizeMB << " MB" << endl;
}

int main(int argc, char **argv){
    Timer tim;
    HINT_Reconstructable *deadIndex;
    LiveIndex *liveIndex;
    RunSettings settings;

    size_t maxCapacity = -1, maxNumBuffers = 0;
    size_t totalResult = 0, queryresult = 0, numQueries = 0, numUpdates = 0, numRebuilds = 0;

    Timestamp first, second, startEndpoint, leafPartitionExtent = 0, maxDuration = -1;
    Timestamp Tf = 0;

    double totalBufferStartTime = 0, totalBufferEndTime = 0, totalIndexEndTime = 0, totalRebuildTime = 0;
    double totalQueryTime_b = 0, totalQueryTime_i = 0, totalQueryTimeFossil = 0;
    double unused1, unused2; // Dummy variables consuming the data stream
    double memoryThreshold = 50 * (1024 * 1024);
    
    char operation;
    string typeBuffer, queryFile;
    
    // Parse arguments
    try {
        parseArguments(argc, argv, settings, leafPartitionExtent, typeBuffer, maxCapacity, maxDuration, queryFile);
    } catch (const invalid_argument& e) {
        cerr << "Error: " << e.what() << endl;
        usage();
        return 1;
    }

    // Create indexes
    liveIndex = createLiveIndex(typeBuffer, maxCapacity, maxDuration);
    deadIndex = new HINT_Reconstructable(leafPartitionExtent);
    FossilIndex fossilIndex("fossil_index.db");

    // Load stream
    settings.queryFile = argv[optind];
    ifstream fQ(settings.queryFile);
    if (!fQ){
        usage();
        return 1;
    }

    // Explanation:
    // Operation is S/E to start/end an interval, or Q to query.
    // Fist is the inteval ID if S/E, or query start time if Q.
    // Second is the start/end time if S/E, or query end time if Q.
    // unused1 is the end time but is not used
    // unused2 is only used by aLit. It is probably extra attribute to index.
    while (fQ >> operation >> first >> second >> unused1 >> unused2){
        if (operation == 'S') {
            numUpdates++;
            int id = first;
            Timestamp startTime = second;

            tim.start();
            liveIndex->insert(id, startTime);
            totalBufferStartTime += tim.stop();
        }
        else if (operation == 'E') {
            numUpdates++;
            int id = first;
            Timestamp endTime = second;

            tim.start();
            startEndpoint = liveIndex->remove(id); // This returns the start timestamp of the deleted interval
            totalBufferEndTime += tim.stop();

            tim.start();
            deadIndex->insert(Record(id, startEndpoint, endTime));
            totalIndexEndTime += tim.stop();

            // Fossilize intervals
            if (liveIndex->getMemoryUsage() + deadIndex->getMemoryUsage() > memoryThreshold) {
                tim.start();
                Tf += (endTime - Tf) / 2;
                const Relation& fossilIntervals = deadIndex->rebuild(Tf);
                if (fossilIntervals.size() > 0) {
                    for (const auto& interval : fossilIntervals)
                        fossilIndex.insertInterval(interval.id, interval.start, interval.end);
                    totalRebuildTime += tim.stop();
                    numRebuilds++;
                }
            }
        }
        else if (operation == 'Q') {
            Timestamp qStart = first;
            Timestamp qEnd = second;
            numQueries++;

            for (auto r = 0; r < settings.numRuns; r++){
                tim.start();
                // Question: Why does the query takes numQueries and uses it as id?
                queryresult = liveIndex->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
                totalQueryTime_b += tim.stop();
                // Question: Is buffer time synonymous to live index time?

                tim.start();
                if (qStart <= deadIndex->gend){
                    cout << "Querying dead" << endl;
                    queryresult ^= deadIndex->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
                }
                totalQueryTime_i += tim.stop();

                tim.start();
                if (qStart <= Tf){
                    cout << "Querying fossil" << endl;
                    queryresult += fossilIndex.query(qStart, qEnd);
                }
                totalQueryTimeFossil += tim.stop();
            }
            totalResult += queryresult;
        }
        maxNumBuffers = max(maxNumBuffers, liveIndex->getNumBuffers());
    }
    fQ.close();

    // Report
    deadIndex->getStats();
    cout << endl << "fossilLIT" << endl;
    cout << "====================" << endl << endl;
    cout << "Buffer info" << endl;
    cout << "Type                               : " << typeBuffer << endl;
    if (maxCapacity != -1)
        cout << "Buffer capacity                    : " << maxCapacity << endl;
    else
        cout << "Buffer duration                    : " << maxDuration << endl;
    cout << "Index info" << endl;
    cout << "Updates report" << endl;
    cout << "Num of updates                     : " << numUpdates << endl;
    cout << "Num of buffers  (max)              : " << maxNumBuffers << endl;
    cout << "Num of rebuilds                    : " << numRebuilds << endl;
    cout << "Total time for rebuilds [secs]     : " << totalRebuildTime << endl;
    cout << "Total updating time (buffer) [secs]: " << (totalBufferStartTime + totalBufferEndTime) << endl;
    cout << "Total updating time (index)  [secs]: " << totalIndexEndTime << endl;
    cout << "Num of fossils                     : " << fossilIndex.getObjectCount() << endl << endl;

    cout << "Queries report" << endl;
    cout << "Num of queries                     : " << numQueries << endl;
    cout << "Num of runs per query              : " << settings.numRuns << endl;
    cout << "Total result [XOR]                 : " << totalResult << endl;
    cout << "Total querying time (buffer) [secs]: " << (totalQueryTime_b / settings.numRuns) << endl;
    cout << "Total querying time (index)  [secs]: " << (totalQueryTime_i / settings.numRuns) << endl;
    cout << "Total querying time (fossil) [secs]: " << (totalQueryTimeFossil / settings.numRuns) << endl;

    displayMemoryUsage(liveIndex, deadIndex, fossilIndex);
    return 0;
}