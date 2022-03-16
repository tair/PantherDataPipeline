cd $1
wget $2
echo Unzipping tsv files...
tar -xzvf *.tar.gz
echo Finished unzipping tsv tar files.
echo Downloading obo file...
wget $3
echo Finished downloading obo file.