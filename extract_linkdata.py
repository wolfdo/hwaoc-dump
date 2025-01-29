import argparse
import math
import os
import struct
import zlib

# Constants
FIELD_ALIGNMENT = 128
COMPRESSION_ZLIB = 1

# Structs
INDEX_ENTRY = struct.Struct('<QQQQ4s4s')
DATA_ENTRY_HEADER = struct.Struct('<IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII')


def read_index_entries(fp_idx):
    """Reads index entries from the index file."""
    fp_idx.seek(0, os.SEEK_END)
    idx_file_size = fp_idx.tell()
    fp_idx.seek(0)

    entries = []
    while fp_idx.tell() < idx_file_size:
        try:
            entry = INDEX_ENTRY.unpack(fp_idx.read(INDEX_ENTRY.size))
            entries.append(entry)
        except struct.error:
            print(f'WARNING: Corrupt index entry at position {fp_idx.tell()}. Skipping...')
            break

    return entries, idx_file_size


def decompress_block(block_data, block_id):
    """Handles decompression of a data block."""
    try:
        header_fields = DATA_ENTRY_HEADER.unpack(block_data[:DATA_ENTRY_HEADER.size])
        field_sizes = list(filter(lambda s: s > 0, header_fields[3:]))

        if header_fields[1] > len(field_sizes):
            # TODO: handle
            print(f'WARNING: Field overflow in block {block_id}. Skipping...')
            return None

        start_offset = DATA_ENTRY_HEADER.size
        decompressed_data = bytearray()

        for field_size in field_sizes:
            end_offset = start_offset + field_size
            field_payload = block_data[start_offset:end_offset]

            # Extract inner field size
            inner_field_size, = struct.unpack('<I', field_payload[:4])
            if field_size - 4 != inner_field_size:
                # TODO: handle
                print(f'WARNING: Mismatch in field size ({inner_field_size} != {field_size - 4}) in block {block_id}')

            decompressed_data.extend(zlib.decompress(field_payload[4:]))

            # Align to next field
            start_offset = ((end_offset + FIELD_ALIGNMENT - 1) // FIELD_ALIGNMENT) * FIELD_ALIGNMENT

        return decompressed_data

    except zlib.error as e:
        print(f'ERROR: Decompression failed for block {block_id}: {e}')
        return None


def extract_blocks(idx_file, data_file, output_base_path):
    """Extracts and processes blocks from the index and data files."""
    os.makedirs(output_base_path, exist_ok=True)

    with idx_file as fp_idx:
        entries, idx_file_size = read_index_entries(fp_idx)

    filename_digits = math.ceil(math.log10(len(entries))) if entries else 1

    with data_file as fp_data:
        fp_data.seek(0, os.SEEK_END)
        data_file_size = fp_data.tell()

        # TODO: last two entries are probably some data type and checksum
        for block_id, (offset, uncompressed_size, compressed_size, compression, _, _) in enumerate(entries, start=1):
            if offset + compressed_size > data_file_size:
                print(
                    f'ERROR: Invalid entry at block {block_id}: offset {offset} + compressed_size {compressed_size} exceeds data file size.')
                continue

            fp_data.seek(offset)
            block_data = fp_data.read(compressed_size)

            if compression == COMPRESSION_ZLIB:
                decompressed_data = decompress_block(block_data, block_id)
                if decompressed_data is not None:
                    block_data = decompressed_data

            output_filename = os.path.join(output_base_path, f'{block_id:0{filename_digits}d}.bin')
            with open(output_filename, 'wb') as fp_block:
                fp_block.write(block_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract blocks from IDX and DATA files.')
    parser.add_argument('--idx_file', type=argparse.FileType('rb'), default='LinkInfo.bin', help='Path to index file')
    parser.add_argument('--data_file', type=argparse.FileType('rb'), default='LinkData.bin', help='Path to data file')
    parser.add_argument('--output_path', type=str, default='output', help='Output directory')

    args = parser.parse_args()
    extract_blocks(args.idx_file, args.data_file, args.output_path)
