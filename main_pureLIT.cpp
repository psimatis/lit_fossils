#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/live_index.cpp"
#include "./indices/hint_m.h"

using namespace std;

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

void displayMemoryUsage(LiveIndex* liveIndex, HINT_M_Dynamic* deadIndex) {
    size_t liveIndexSize = liveIndex->getMemoryUsage();
    size_t deadIndexSize = deadIndex->getMemoryUsage();

    double liveIndexSizeMB = liveIndexSize / (1024.0 * 1024.0);
    double deadIndexSizeMB = deadIndexSize / (1024.0 * 1024.0);

    cout << "\nSize Report:" << endl;
    cout << "Live Index Memory Usage            : " << liveIndexSizeMB << " MB" << endl;
    cout << "Dead Index Memory Usage            : " << deadIndexSizeMB << " MB" << endl;
}

int main(int argc, char **argv){
    Timer tim;
    HINT_M_Dynamic *deadIndex;
    LiveIndex *liveIndex;
    RunSettings settings;

    size_t maxCapacity = -1, maxNumBuffers = 0;
    size_t totalResult = 0, queryresult = 0, numQueries = 0, numUpdates = 0;

    Timestamp first, second, startEndpoint, leafPartitionExtent = 0, maxDuration = -1;
    Timestamp Tf = 0;

    double totalBufferStartTime = 0, totalBufferEndTime = 0, totalIndexEndTime = 0;
    double totalQueryTime_b = 0, totalQueryTime_i = 0;
    double unused1, unused2; // Dummy variables consuming the data stream
    double memoryThreshold = 50 * (1024 * 1024);
    
    char operation;
    string typeBuffer, queryFile;
    
    // Parse arguments
    try {
        parseArguments(argc, argv, settings, leafPartitionExtent, typeBuffer, maxCapacity, maxDuration, queryFile);
    } catch (const invalid_argument& e) {
        cerr << "Error: " << e.what() << endl;
        usage("pureLIT");
        return 1;
    }

    // Create indexes
    liveIndex = createLiveIndex(typeBuffer, maxCapacity, maxDuration);
    deadIndex = new HINT_M_Dynamic(leafPartitionExtent);

    // Load stream
    settings.queryFile = argv[optind];
    ifstream fQ(settings.queryFile);
    if (!fQ){
        usage("pureLIT");
        return 1;
    }

    bool flag = true;
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
            startEndpoint = liveIndex->remove(id); 
            totalBufferEndTime += tim.stop();

            tim.start();
            deadIndex->insert(Record(id, startEndpoint, endTime));
            totalIndexEndTime += tim.stop();
        }
        else if (operation == 'Q') {
            Timestamp qStart = first;
            Timestamp qEnd = second;
            numQueries++;

            for (auto r = 0; r < settings.numRuns; r++){
                tim.start();
                queryresult = liveIndex->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
                totalQueryTime_b += tim.stop();

                tim.start();
                if (qStart <= deadIndex->gend)
                    queryresult ^= deadIndex->execute_pureTimeTravel(RangeQuery(numQueries, qStart, qEnd));
                totalQueryTime_i += tim.stop();
            }
            totalResult += queryresult;
        }
        maxNumBuffers = max(maxNumBuffers, liveIndex->getNumBuffers());
    }
    fQ.close();

    // Report
    cout << endl << "purelLIT" << endl;
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
    cout << "Total updating time (buffer) [secs]: " << (totalBufferStartTime + totalBufferEndTime) << endl;
    cout << "Total updating time (index)  [secs]: " << totalIndexEndTime << endl;

    cout << "Queries report" << endl;
    cout << "Num of queries                     : " << numQueries << endl;
    cout << "Num of runs per query              : " << settings.numRuns << endl;
    cout << "Total result [XOR]                 : " << totalResult << endl;
    cout << "Total querying time (buffer) [secs]: " << (totalQueryTime_b / settings.numRuns) << endl;
    cout << "Total querying time (index)  [secs]: " << (totalQueryTime_i / settings.numRuns) << endl;

    displayMemoryUsage(liveIndex, deadIndex);

    return 0;
}