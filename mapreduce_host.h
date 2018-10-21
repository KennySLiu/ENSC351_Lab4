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
#define NUM_CORES 12 //4 for the laptop, 12 for pit machines and home computer


template <typename keytype, typename valuetype>
struct threaded_map_func_input{
    int thread_num;
    std::pair<keytype, valuetype> (*map_func) (keytype);
    std::vector<keytype>* parsed_input;
    std::vector<std::pair<keytype, valuetype> >* ret_p;
};

template<class keytype, class valuetype>
struct threaded_reduce_func_input{
    int thread_num;
    std::pair<keytype, valuetype> (*reduce_func) (std::vector<std::pair<keytype, valuetype> >);
    std::vector<std::pair<keytype, valuetype> >* ret_p;
    std::vector<std::pair<keytype, valuetype> >* mapped_kv_pairs_subVect;
    std::vector<int>* delimiter_indices;
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
    vector<keytype>* parsed_input = input->parsed_input;
    vector<pair<keytype, valuetype> >* ret_p = input->ret_p;

    for (int i = thread_num; i < parsed_input->size(); i += NUM_CORES){
        pair<keytype, valuetype> to_push = map_func((*parsed_input)[i]);
        (*ret_p).push_back(to_push);
    }
}   

template<class keytype, class valuetype>
void* threaded_reduce_func(void* vp_input){
    using namespace std;
    struct threaded_reduce_func_input<keytype, valuetype>* input = static_cast<struct threaded_reduce_func_input<keytype, valuetype>* >(vp_input);
    
    int thread_num = input->thread_num;
    pair<keytype, valuetype> (*reduce_func) (vector<pair<keytype, valuetype> >) = input->reduce_func;
    vector<pair<keytype, valuetype> >* ret_p = input->ret_p;
    vector<pair<keytype, valuetype> >* mapped_kv_pairs_subVect = input->mapped_kv_pairs_subVect;
    vector<int>* delimiter_indices = input->delimiter_indices;

    if (delimiter_indices->size() == 0){
        return NULL;
    }

    for (int i = 0; i < delimiter_indices->size()-1; ++i){

        // Build the sub-vector that we will pass into the reducer:
        vector<pair<keytype, valuetype> > cur_subVect;
        for (int j = (*delimiter_indices)[i]; j < (*delimiter_indices)[i+1]; ++j){
            cur_subVect.push_back((*mapped_kv_pairs_subVect)[j]);
        }
        
        pair<keytype, valuetype> cur_pair = reduce_func(cur_subVect);
        ret_p->push_back(cur_pair);
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

    
    threaded_map_func_input<keytype, valuetype>* mapper_inputs[NUM_CORES];

    for (int i = 0; i < NUM_CORES; ++i){
        // Create the input to the threaded_map_func:
        mapper_inputs[i] = new threaded_map_func_input<keytype, valuetype>;
        mapper_inputs[i]->thread_num = i;
        mapper_inputs[i]->map_func = map_func;
        mapper_inputs[i]->parsed_input = &parsed_input;
        mapper_inputs[i]->ret_p = &threaded_mapfunc_retvals[i];

        // And create a thread to run the function.
        pthread_create(&threads[i], NULL, threaded_map_func<keytype, valuetype>, static_cast<void*>(mapper_inputs[i]));
    }

    for (int i = 0; i < NUM_CORES; ++i){
        // Join the threads, and then get their values
        pthread_join(threads[i], NULL);
        //vector<pair<keytype, valuetype> > mapped_kv_subVect = threaded_mapfunc_retvals[i];
        for (int j = 0; j < threaded_mapfunc_retvals[i].size(); ++j){
            pair<keytype, valuetype> cur;
            cur = threaded_mapfunc_retvals[i][j];
            mapped_kv_pairs.push_back(cur);
        }
    }
    for (int i = 0; i < NUM_CORES; ++i){
        delete mapper_inputs[i];
    }
    /* END OF MAPPER SECTION */



    /* Sort the vector: */
    sort(mapped_kv_pairs.begin(), mapped_kv_pairs.end(), mapvect_sort_func<keytype, valuetype>);

    

    /* REDUCER: */

    // For each thread, we will pass in their mapped_kv_pairs_subVect AND their delimiter indices, so they 
    // don't have to recompute the chunks of the vectors that are the same.
    // We also pass in a threaded_reduce_func_retvals vector for them to write their results to.
    vector<pair<keytype, valuetype> > threaded_reduce_func_retvals[NUM_CORES];
    vector<pair<keytype, valuetype> > mapped_kv_pairs_subVect[NUM_CORES];
    vector<int> delimiter_indices[NUM_CORES];

    for (int i = 0; i < mapped_kv_pairs.size(); ++i){
        // We want to push the mapped kv pairs evenly to each thread. To do so, we check the workloads so-far-created for each thread.
        // Then we pick the one with the shortest workload.
        vector<pair<keytype, valuetype> >* target;
        size_t minsize = 999999;
        int minsize_index = 0;
        int j = 0;

        for (int minsize_it = 0; minsize_it < NUM_CORES; ++minsize_it){
            size_t curVectSize = mapped_kv_pairs_subVect[minsize_it].size();

            if (curVectSize < minsize){
                minsize = curVectSize;
                minsize_index = minsize_it;
            }
        } 
        target = &mapped_kv_pairs_subVect[minsize_index];
        // Finished finding the thread with the least workload.
    
        // Run until the j'th key is not equal to the i'th key:
        for (j = i; j < mapped_kv_pairs.size() && mapped_kv_pairs[j].first == mapped_kv_pairs[i].first; ++j){
            // Copy the elements that match one another
            pair<keytype, valuetype> cur = mapped_kv_pairs[j];
            target->push_back(cur);
        }
        i = j - 1;
        // Delimit based on the changes in size. The first time we push back 0 and size(), and every other time we push back size().
        // This allows the threaded functions to get the subvector from delim_ind[0] and delim_ind[1]-1, and then from delim_ind[1] to delim_ind[2]-1, and so on.
        if (delimiter_indices[minsize_index].size() == 0){
            delimiter_indices[minsize_index].push_back(0);
        }
        delimiter_indices[minsize_index].push_back( mapped_kv_pairs_subVect[minsize_index].size() );
    }

    
    threaded_reduce_func_input<keytype, valuetype>* reducer_inputs[NUM_CORES];
    
    for (int i = 0; i < NUM_CORES; ++i){
        reducer_inputs[i] = new threaded_reduce_func_input<keytype, valuetype>;
        reducer_inputs[i]->thread_num = i;
        reducer_inputs[i]->reduce_func = reduce_func;
        reducer_inputs[i]->ret_p = &threaded_reduce_func_retvals[i];
        reducer_inputs[i]->mapped_kv_pairs_subVect = &mapped_kv_pairs_subVect[i];
        reducer_inputs[i]->delimiter_indices = &delimiter_indices[i];

        pthread_create(&threads[i], NULL, threaded_reduce_func<keytype, valuetype>, static_cast<void*>(reducer_inputs[i]));
    } 

    vector<pair<keytype, valuetype> > reduced_kv_pairs;

    for (int i = 0; i < NUM_CORES; ++i){
        pthread_join(threads[i], NULL);

        for (int j = 0; j < threaded_reduce_func_retvals[i].size(); ++j){
            pair<keytype, valuetype> cur_pair = (threaded_reduce_func_retvals[i])[j];
            reduced_kv_pairs.push_back(cur_pair);
        } 
    }
    
    for (int i = 0; i < NUM_CORES; ++i){
        delete reducer_inputs[i];
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
