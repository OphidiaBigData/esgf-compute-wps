#!/bin/bash

# capabilities
capabilities=$(cat capabilitiesAndDescriptions/capabilities)
cap_status=1
cap_host="'default'"
cap_added_date="$(date "+'%Y-%m-%d %H:%M:%S.%N %z'")"

# insert capabilities #change password
cap_newid=$(PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_server (host, added_date, status, capabilities) VALUES ($cap_host,$cap_added_date,$cap_status,$capabilities) RETURNING id;")

#describe process

# max
identifier="'OPHIDIA.max'"
description=$(cat capabilitiesAndDescriptions/ophMaxDesc)
backend="'local'"

# insert max 
max_newid=$(PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_process (identifier, description, backend) VALUES ($identifier,$description,$backend) RETURNING id;")

#min
identifier="'OPHIDIA.min'"
description=$(cat capabilitiesAndDescriptions/ophMinDesc)
backend="'local'"

# insert min 
min_newid=$(PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_process (identifier, description, backend) VALUES ($identifier,$description,$backend) RETURNING id;")


#subset
identifier="'OPHIDIA.subset'"
description=$(cat capabilitiesAndDescriptions/ophSubsetDesc)
backend="'local'"

# insert subset 
subset_newid=$(PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_process (identifier, description, backend) VALUES ($identifier,$description,$backend) RETURNING id;")


# insert relations
#max
PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_server_processes (server_id, process_id) VALUES ($cap_newid,$max_newid);"

#min
PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_server_processes (server_id, process_id) VALUES ($cap_newid,$min_newid);"

#subset
PGPASSWORD='abcd' psql -U postgres  -d postgres -t -X -q -c "INSERT INTO wps_server_processes (server_id, process_id) VALUES ($cap_newid,$subset_newid);"



