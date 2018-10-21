#ifndef ENSC351_MAPREDUCE_HOST
#define ENSC351_MAPREDUCE_HOST

#include "kenny_include.h"
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>
#include <utility>
#include <algorithm>
#include <vector>
#define NUM_CORES 12 //4 for the laptop, 12 for pit machines


template <typename keytype, typename valuetype>
struct threaded_map_func_input{
    int thread_num;
    std::pair<keytype, valuetype> (*map_func) (keytype);
    std::vector<keytype> parsed_input;
    std::vector<std::pair<keytype, valuetype> >* ret_p;
};

template<class keytype, class valuetype>
bool mapvect_sort_func(std::pair<keytype, valuetype> a, std::pair<keytype, valuetype> b){
    return (a.first < b.first);
}

template<class keytype, class valuetype>
void* threaded_map_func(void* vp_input){
    using namespace std;

    // Grab the input values from the void pointer:
    struct threaded_map_func_input<keytype, valuetype>* input = static_cast<struct threaded_map_func_input<keytype, valuetype>* >(vp_input);
    int thread_num = input->thread_num;
    pair<keytype, valuetype> (*map_func) (keytype) = input->map_func;
    vector<keytype> parsed_input = input->parsed_input;
    vector<pair<keytype, valuetype> >* ret_p = input->ret_p;

    for (int i = thread_num; i < parsed_input.size(); i += NUM_CORES){
        pair<keytype, valuetype> to_push = map_func(parsed_input[i]);
        (*ret_p).push_back(to_push);
    }
}   



template<class keytype, class valuetype>
void mapreduce(std::vector<keytype> input_reader (void*), std::pair<keytype, valuetype> map_func (keytype), std::pair<keytype, valuetype> reduce_func (std::vector<std::pair<keytype, valuetype> >), void* output_func (std::vector<std::pair<keytype, valuetype> >), void* file){
    using namespace std;
    
    /* INPUT READER: */
    vector<keytype> parsed_input = input_reader(file);
    /* END OF INPUT READER SECTION */


    /* MAPPER: */
    vector<pair<keytype, valuetype> > mapped_kv_pairs;
    void* ret;
    pthread_t threads[NUM_CORES];
    vector<pair<keytype, valuetype> > threaded_mapfunc_retvals[NUM_CORES];

    for (int i = 0; i < NUM_CORES; ++i){
        // Create the input to the threaded_map_func:
        threaded_map_func_input<keytype, valuetype>* input = new threaded_map_func_input<keytype, valuetype>;
        (*input).thread_num = i;
        (*input).map_func = map_func;
        (*input).parsed_input = parsed_input;
        (*input).ret_p = &threaded_mapfunc_retvals[i];

        // And create a thread to run the function.
        pthread_create(&threads[i], NULL, threaded_map_func<keytype, valuetype>, static_cast<void*>(input));
    }

    for (int i = 0; i < NUM_CORES; ++i){
        // Join the threads, so we can get their values
        pthread_join(threads[i], NULL);
        vector<pair<keytype, valuetype> > mapped_kv_subVect_p = threaded_mapfunc_retvals[i];
        for (int j = 0; j < mapped_kv_subVect_p.size(); ++j){
            pair<keytype, valuetype> cur = mapped_kv_subVect_p[i];
            mapped_kv_pairs.push_back(cur);
        }
    }
    /* END OF MAPPER SECTION */



    /* Sort the vector: */
    sort(mapped_kv_pairs.begin(), mapped_kv_pairs.end(), mapvect_sort_func<keytype, valuetype>);

    

    /* REDUCER: */
    vector<pair<keytype, valuetype> > reduced_kv_pairs;

    for (int i = 0; i < mapped_kv_pairs.size(); ++i){
        vector<pair<keytype, valuetype> > mapped_kv_pairs_subVect;
    
        // Run until the j'th key is not equal to the i'th key:
        for (int j = i; j < mapped_kv_pairs.size() && static_cast<keytype>(mapped_kv_pairs[j].first) == static_cast<keytype>(mapped_kv_pairs[i].first); ++j){
            // Copy the elements that match one another
            pair<keytype, valuetype> cur = mapped_kv_pairs[j];
            mapped_kv_pairs_subVect.push_back(cur);
            i = j;
        }

        // Pass in the subVector to reduced_kv_pairs.
        reduced_kv_pairs.push_back(reduce_func(mapped_kv_pairs_subVect));
    }
    /* END OF REDUCER SECTION */



    /* Debug: 
    for (int i = 0; i < reduced_kv_pairs.size(); ++i){
        pair<keytype, valuetype> curpair = reduced_kv_pairs[i];

        keytype curkey = static_cast<keytype>(curpair.first);
        valuetype curval = static_cast<valuetype>(curpair.second);

        cout << curkey << ", " << curval << "\n";
    }
    */

    /* OUTPUTTER: */
    output_func(reduced_kv_pairs);
    /* END OF OUTPUTTER SECTION */
    


}




#endif
