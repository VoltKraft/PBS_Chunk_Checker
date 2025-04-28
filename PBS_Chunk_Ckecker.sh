#!/bin/bash

chunk_list_file = "chunk_list_file"

declare -a file_list
declare -a chunk_list
declare -a chunk_reuse_counter
declare -a chunk_counter


find_files() {
    local search_path="$1"
    while IFS= read -r file; do
        file_list+=("$file")
        echo -ne "\r\033[KIndex found: $file"
    done < <(find "$search_path" -type f \( -name "*.fidx" -o -name "*.didx" \))
}

save_chunks() {
    local in_chunk_section=0
    while IFS= read -r line; do
        if [[ "$line" =~ ^chunks: ]]; then
            in_chunk_section=1
            continue
        fi

        if [[ $in_chunk_section -eq 1 ]]; then
            if [[ "$line" =~ \"([a-f0-9]{64})\" ]]; then
                digest="${BASH_REMATCH[1]}"
                chunk_list+=("$digest") 
                echo -ne "\r\033[KChunk found: $digest | Index $i of ${#file_list[@]}"
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
            echo -ne "\r\033[K➕ New: $item"
            unique_array+=("$item")
            seen["$item"]=1
        else
            echo -ne "\r\033[KAlready available: $item"
            ((chunk_reuse_counter++))
        fi
    done
    chunk_list=("${unique_array[@]}")
}

sum_chunk_sizes() {
    local total_size=0
    for i in "${!chunk_list[@]}"; do
        digest="${chunk_list[$i]}" 
        subdir="${digest:0:4}"
        path="$CHUNK_PATH/$subdir/$digest"

        if [[ -f "$path" ]]; then
            size=$(du -sb "$path" | cut -f1)
            echo -ne "\r\033[K📦 Chunk $i/${#chunk_list[@]} : $digest → $size Bytes"
            total_size=$((total_size + size))
        else
            echo -ne "\r\033[K❌ Index $i: File not found: $path"
        fi
    done
    clear
    echo "🧮 Total size: $total_size Bytes ($(numfmt --to=iec-i --suffix=B $total_size))"
}

check_folder_exists() {
    local folder_path="$1"
    echo "❓ Check if folder $folder_path exists"
    if [[ -d "$folder_path" ]]; then
        echo "✅ Folder exists → $folder_path"
        return 0
    else
        echo "❌ Error: Folder does not exist → $folder_path" >&2
        exit 1
    fi
}

get_datastore_path() {
    local datastore_name="$1"
    local output
    local path
    output=$(proxmox-backup-manager datastore show "$datastore_name" --output-format json 2>/dev/null)
    if [[ -z "$output" ]]; then
        echo "❌ Error: Datastore '$datastore_name' not found or command failed." >&2
        exit 1
    fi
    path=$(echo "$output" | grep -oP '"path"\s*:\s*"\K[^"]+')
    if [[ -n "$path" ]]; then
        echo "$path"
        return 0
    else
        echo "❌ Error: Path could not be extracted for datastore '$datastore_name'" >&2
        exit 1
    fi
}


###################################################################################################


datastore_path=$(get_datastore_path "$1") || exit 1
echo "📁 Path to datastore: $datastore_path"
SEARCH_PATH="$datastore_path$2"
CHUNK_PATH="$datastore_path/.chunks"
echo "📁 Search path: $SEARCH_PATH"
echo "📁 Chunk path: $CHUNK_PATH"

check_folder_exists $SEARCH_PATH

start=$(date +%s)

find_files "$SEARCH_PATH" file_list

for i in "${!file_list[@]}"; do
    save_chunks ${file_list[i]}
done

chunk_counter=${#chunk_list[@]}

remove_duplicates

sum_chunk_sizes

end=$(date +%s)
duration=$((end - start))
hours=$((duration / 3600))
minutes=$(((duration % 3600) / 60))
seconds=$((duration % 60))
echo "⏱️ Evaluation duration: $hours hours, $minutes minutes, and $seconds seconds"

percentage=$((chunk_reuse_counter * 1000 / chunk_counter))
echo "$chunk_reuse_counter/$chunk_counter $((percentage / 10)).$((percentage % 10))% Chunks used several times"
