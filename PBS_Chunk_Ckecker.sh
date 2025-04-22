#!/bin/bash

# Pfad, in dem gesucht werden soll
#SEARCH_PATH="/SSD/ns/Alte_Landstrasse/vm/100/2025-01-05T22:45:03Z"
SEARCH_PATH="$1$2"
#SEARCH_PATH="/test"
CHUNK_PATH="$1/.chunks"


# Array außerhalb der Funktion deklarieren
declare -a file_list
declare -a chunk_list
declare -a chunk_reuse_counter
declare -a chunk_counter

#file_list=("/SSD/ns/Alte_Landstrasse/vm/100/2025-04-17T16:45:01Z/drive-sata0.img.fidx")


# Funktion zum Suchen von .fidx und .didx Dateien
find_files() {
    local search_path="$1"
    
    # Alle .fidx und .didx Dateien mit dem angegebenen Pfad finden und in file_list speichern
    while IFS= read -r file; do
        file_list+=("$file")  # Füge die Datei zum Array hinzu
        echo Datei gefunden: $file
    done < <(find "$search_path" -type f \( -name "*.fidx" -o -name "*.didx" \))
}

# Funktion, die alle Chunks aus einer .fidx-Datei extrahiert und in ein assoziatives Array schreibt
save_chunks() {
    #local fidx_file="$1"
    #local -n chunk_list="$2"  # Referenz auf das Array

    local in_chunk_section=0

    while IFS= read -r line; do
        if [[ "$line" =~ ^chunks: ]]; then
            in_chunk_section=1
            continue
        fi

        if [[ $in_chunk_section -eq 1 ]]; then
            if [[ "$line" =~ \"([a-f0-9]{64})\" ]]; then
                digest="${BASH_REMATCH[1]}"
                chunk_list+=("$digest")  # Fügt den Chunk zum Array hinzu
                echo Chunk gefunden: $digest Index $i von ${#file_list[@]}
            else
                in_chunk_section=0
            fi
        fi
    done < <(proxmox-backup-debug inspect file --output-format text "${file_list[i]}")
}

remove_duplicates() {
    chunk_reuse_counter=0
    local -a unique_array=()
    declare -A seen=()

    for item in "${chunk_list[@]}"; do
        if [[ -z "${seen[$item]}" ]]; then
            echo "➕ Neu: $item"
            unique_array+=("$item")
            seen["$item"]=1
        else
            echo "Bereits vorhanden: $item"
            ((chunk_reuse_counter++))
        fi
    done

    # Rückgabe: überschreibe Originalarray
    chunk_list=("${unique_array[@]}")
}

sum_chunk_sizes() {
    local total_size=0

    # Schleife durch das Array und Berechnung des Index
    for i in "${!chunk_list[@]}"; do
        digest="${chunk_list[$i]}"  # Element (Digest)
        # Subfolder ist das erste zwei Zeichen vom Digest
        subdir="${digest:0:4}"
        path="$CHUNK_PATH/$subdir/$digest"

        if [[ -f "$path" ]]; then
            size=$(du -sb "$path" | cut -f1)
            echo "📦 Chunk $i/${#chunk_list[@]} : $digest → $size Bytes"
            total_size=$((total_size + size))
        else
            echo "❌ Index $i: Datei nicht gefunden: $path"
        fi
    done

    echo "🧮 Gesamtgröße: $total_size Bytes ($(numfmt --to=iec-i --suffix=B $total_size))"
}

###################################################################################################

start=$(date +%s)

find_files "$SEARCH_PATH" file_list

for i in "${!file_list[@]}"; do
    save_chunks ${file_list[i]} chunks
done

chunk_counter=${#chunk_list[@]}

remove_duplicates

sum_chunk_sizes

end=$(date +%s)
duration=$((end - start))
echo "Dauer: $duration Sekunden"

percentage=$((chunk_reuse_counter * 1000 / chunk_counter))
echo "$chunk_reuse_counter/$chunk_counter $((percentage / 10)).$((percentage % 10))% wiederverwertete Chunks"
