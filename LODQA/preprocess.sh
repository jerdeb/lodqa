#!/bin/bash

# variable holding the data URI
data=$1
useProxy=$2;
filename=$3
folder='/tmp/';

echo "Parameters: $data $useProxy $filename";

tempPreProcess="/tmp/$filename";

if [ -f "$folder$filename.nt.gz" ]; then
	echo "Datadump available in: $folder$filename.nt.gz";
else
	mkdir $tempPreProcess;

	export IFS=","
	for f in $data; do
		echo "Downloading data dump from: $f";
		cd "$tempPreProcess";

		if [ $useProxy == "true" ]; then
			file=$(LANG=C wget "$f" -e use_proxy=yes -e http_proxy=webcache.iai.uni-bonn.de:3128 -P $tempPreProcess 2>&1 | sed -n "s/Saving to: '\(.*\)'/\1/p") ;
		else
			file=$(LANG=C wget "$f" -P $tempPreProcess 2>&1 | sed -n "s/Saving to: '\(.*\)'/\1/p") ;
		fi

		cd "$tempPreProcess";
		if [[ $file == *".html"* ]]
		then
		  mv "$file" "$(uuidgen)"
		fi

	    if [[ $file == *"?"* ]]; then
		  mv "$file" "$(uuidgen)"
		fi

		if [[ $file =~ \.tar$ ]];then
			echo "Extract .tar file";
			tar -xvf $file
			rm -rf $file ;
		elif [[ $file =~ \.tar.gz$ ]];then
			echo "Extract .tar.gz file";
			tar -xzvf $file
			rm -rf $file ;
		elif [[ $file =~ \.tar.bz$ ]];then
			echo "Extract .tar.bz file";
			tar -xzvf $file
			rm -rf $file ;
		elif [[ $file =~ \.gz$ ]];then
			echo "Extract .gz file";
			gunzip $file
			rm -rf $file ;
		elif [[ $file =~ \.zip$ ]];then
			echo "Extract .zip file";
			echo $file
			unzip $file
			rm -rf $file ;
		elif [[ $file =~ \.bz2$ ]];then
			echo "Extract .bz2 file";
			bzip2 -dk $file
			rm -rf $file ;
		elif [[ $file =~ \.bz2$ ]];then
			echo "Extract .bz2 file";
			bzip2 -dk $file
			rm -rf $file ;
        elif [[ $file =~ \.tgz$ ]];then
            echo "Extract .tgz file";
            tar -xvzf $file
            rm -rf $file ;
		fi
	done

	cd "$tempPreProcess"
	IFS=$'\n';
	for f in $(find $tempPreProcess -name '*.*'); do
		mv "$f" "$tempPreProcess"
	done

	for f in *; do
		if [[ $f =~ \.nt$ ]];then
			echo "Sorting Triples...";
			sort -u "$f" > "$tempPreProcess/$f-sorted".nt
			# mv "$filename_sorted.nt" > "$filename.nt"
		else
		   if [[ $f =~ \.ttl$ ]]; then
			echo "Converting Turtle file using SERDI";
			serdi -b -f -i turtle -o ntriples -o ntriples "$f" > "$f".nt;
			sort -m "$f".nt > "$tempPreProcess/$f-sorted".nt;
		   elif [[ $f =~ \.xml$ ]]; then
			  mv "$f" "$f.rdf"
  			  echo "Converting file to NTriples and Sorting Triples...";
  			  rapper -i guess -o ntriples "$f.rdf" > "$f".nt;
  			  sort -m "$f".nt > "$tempPreProcess/$f-sorted".nt;
		  else
			echo "Converting file to NTriples and Sorting Triples...";
			rapper -i guess -o ntriples "$f" > "$f".nt;
			sort -m "$f".nt > "$tempPreProcess/$f-sorted".nt;
		  fi
		fi
	done


	# if we have just one file don't merge/sort
	cnt=$(ls | wc -l)

	if [ $cnt -gt 1 ]; then
		echo "Merging Sorted Files...";
		cat *-sorted.nt > "merged.nt";
		echo "Cleaning...";
		sort -u "merged.nt" > "cleaned.nt" ;

		echo "$folder$filename.nt";
		mv "cleaned.nt" "$folder$filename.nt";
	else
		mv "$tempPreProcess/$f-sorted".nt "$folder$filename.nt";
	fi

	echo "Gzipping NT file"
	gzip "$folder$filename.nt"

	#rm -rf "$tempPreProcess"

	echo "Datadump available in: $folder$filename.nt.gz";
fi
