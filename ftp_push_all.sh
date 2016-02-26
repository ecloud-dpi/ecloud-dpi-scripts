# $1 dir $2 file
ftp -n -v -i <<EOF
open 10.142.80.21
user BSS-Jizhong H6g!52et
cd /dpi/scripts
binary
mput *
close
bye

