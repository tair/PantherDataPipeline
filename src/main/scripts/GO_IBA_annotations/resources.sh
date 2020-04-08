cd $1
wget $2/*.gaf.gz
echo Unzipping gaf files...
gunzip *.gaf.gz
echo Finished unzipping gaf files.
echo Downloading obo file...
wget $3
echo Finished downloading obo file.