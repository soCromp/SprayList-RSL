#include "rsl.h"
#include <stdio.h>

extern "C" {
    typedef std::pair<const slkey_t, val_t> value_type;
    
    rsl_t *rsl_create(setup_t *cfg){
        if(cfg->layers == 0) { //calculate layers
            size_t key_range = 65536;
            int chunksize = 32;
            int index_size = 32;
            const double data_node_count_target = key_range * 1.0 / chunksize;
            int layers = static_cast<int>(
                std::ceil(std::log(data_node_count_target) / std::log(index_size)));
            layers = layers < 0 ? 1 : layers;
            cfg->layers = layers;
        }
        rsl_t *r = new rsl(cfg);
        return r;
    };

    //tid is thread's id
    int rsl_insert(rsl_t *pq, slkey_t k, val_t v, int tid) {
        slkey_t ktrans = (k << 32)+tid;
        const value_type ins = {ktrans, v};
        return (int) pq->insert(ins); //0 == false, 1 == true in C++ and C
    };

    int rsl_extract_min(rsl_t *pq, slkey_t *k, val_t *v, int tid) {
        int res = (int) pq->extract_min_concur(k, v, tid);
        if(res == 1) 
            *k = *k >> 32;
        return res;
    };

    int rsl_size(rsl_t *pq) {
        return pq->get_size();
    }

}
