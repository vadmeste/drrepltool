
### re-replicate from DR to primary

#### create a list of content to be re-replicated in /tmp/data/object_listing.txt
./drrepltool list --endpoint http://DR:9002 --ak minio --sk minio123 --versions --bucket bucket --prefix pfx --d /tmp/data

### Now do a copy from dr -> primary
./drrepltool copy --endpoint http://primary:9002 --ak minio --sk minio123 --src-endpoint http://dr:9004 --src-access-key minio --src-secret-key minio123 --data-dir /tmp/data --bucket bucket --src-bucket bucket