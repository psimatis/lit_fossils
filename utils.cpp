#include "def_global.h"



string toUpperCase(char *buf)
{
    auto i = 0;
    while (buf[i])
    {
        buf[i] = toupper(buf[i]);
        i++;
    }
    
    return string(buf);
}


bool checkPredicate(string strPredicate, RunSettings &settings)
{
    if (strPredicate == "EQUALS")
    {
        settings.typePredicate = PREDICATE_EQUALS;
        return true;
    }
    else if (strPredicate == "STARTS")
    {
        settings.typePredicate = PREDICATE_STARTS;
        return true;
    }
    else if (strPredicate == "STARTED")
    {
        settings.typePredicate = PREDICATE_STARTED;
        return true;
    }
    else if (strPredicate == "FINISHES")
    {
        settings.typePredicate = PREDICATE_FINISHES;
        return true;
    }
    else if (strPredicate == "FINISHED")
    {
        settings.typePredicate = PREDICATE_FINISHED;
        return true;
    }
    else if (strPredicate == "MEETS")
    {
        settings.typePredicate = PREDICATE_MEETS;
        return true;
    }
    else if (strPredicate == "MET")
    {
        settings.typePredicate = PREDICATE_MET;
        return true;
    }
    else if (strPredicate == "OVERLAPS")
    {
        settings.typePredicate = PREDICATE_OVERLAPS;
        return true;
    }
    else if (strPredicate == "OVERLAPPED")
    {
        settings.typePredicate = PREDICATE_OVERLAPPED;
        return true;
    }
    else if (strPredicate == "CONTAINS")
    {
        settings.typePredicate = PREDICATE_CONTAINS;
        return true;
    }
    else if (strPredicate == "CONTAINED")
    {
        settings.typePredicate = PREDICATE_CONTAINED;
        return true;
    }
    else if (strPredicate == "BEFORE")
    {
        settings.typePredicate = PREDICATE_PRECEDES;
        return true;
    }
    else if (strPredicate == "AFTER")
    {
        settings.typePredicate = PREDICATE_PRECEDED;
        return true;
    }
    if (strPredicate == "GOVERLAPS")
    {
        settings.typePredicate = PREDICATE_GOVERLAPS;
        return true;
    }

    return false;
}

bool checkAttributeConstraint(string strSecondAttributeConstraint, RunSettings &settings)
{
    if (strSecondAttributeConstraint == "GREATER")
    {
        settings.typeSecondAttributeConstraint = SECOND_ATTR_GREATERTHAN;
        return true;
    }
    else if (strSecondAttributeConstraint == "LOWER")
    {
        settings.typeSecondAttributeConstraint = SECOND_ATTR_LOWERTHAN;
        return true;
    }

    return false;
}

bool checkOptimizations(string strOptimizations, RunSettings &settings)
{
    if (strOptimizations == "")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_NO;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT;
        return true;
    }
    else if (strOptimizations == "SUBS+SOPT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SOPT;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT+SS")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT+CM")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM;
        return true;
    }
    else if (strOptimizations == "SUBS+CM")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_CM;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SS+CM")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SS_CM;
        return true;
    }
    else if (strOptimizations == "ALL")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_ALL;
        return true;
    }
    else if (strOptimizations == "SS")
    {
        settings.typeOptimizations = HINT_OPTIMIZATIONS_SS;
        return true;
    }


    return false;
}


void process_mem_usage(double& vm_usage, double& resident_set)
{
    vm_usage     = 0.0;
    resident_set = 0.0;
    
    // the two fields we want
    unsigned long vsize;
    long rss;
    {
        std::string ignore;
        std::ifstream ifs("/proc/self/stat", std::ios_base::in);
        ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
        >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
        >> ignore >> ignore >> vsize >> rss;
    }
    
    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}

// Display instructions
void usage(string indexName){
    cerr << endl << "PROJECT" << endl;
    cerr << "       LIT Variant: " << indexName << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_" << indexName << ".exec [OPTIONS] [STREAMFILE]" << endl << endl;
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
    cerr << "       ./query_" << indexName << ".exec -e 86400 -b ENHANCEDHASHMAP -c 10000 streams/BOOKS.mix" << endl << endl;
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
