make clean
make all
./query_fossilLIT_recon.exec -e 86400 -b ENHANCEDHASHMAP -c 10000 streams/BOOKS.mix
rm fossil_index.db.*
./query_fossilLIT_delete.exec -e 86400 -b ENHANCEDHASHMAP -c 10000 streams/BOOKS.mix