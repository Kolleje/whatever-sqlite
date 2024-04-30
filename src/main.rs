// use std::{fs::File, io::Read};

use std::{fs::File, io::Seek};

use helper::{read_u16, read_u32};

use crate::helper::read_var_int;
// use std::io::{Read, Seek};
const HEADER_SIZE: usize = 100;
const PAGE_SIZE: usize = 4096;
const SQLITE_HEADER_STRING: &str = "SQLite format 3\0";
const FILENAME: &str = "./mostbasic3.sqlite";

mod helper;
// mod lib;

struct SqliteHeader {
    version_string: String,
    page_size: u16,
    // version_valid_for: u64,
    version: u32,
}

impl SqliteHeader {
    fn new(buf: &[u8; 100]) -> SqliteHeader {
        let page_size = read_u16(buf, 16);
        let version = read_u32(buf, 96);
        let string_bytes = &buf[..16];
        let version_string = String::from_utf8(string_bytes.into()).unwrap();
        println!(
            "partition: {},{}, {}, {}",
            page_size,
            version,
            version_string,
            SQLITE_HEADER_STRING == &version_string
        );
        if SQLITE_HEADER_STRING != version_string {
            panic!("invalid file: header string missmatch");
        }
        SqliteHeader {
            page_size,
            version,
            version_string,
        }
    }
}

enum BTreeHeader {
	Leaf(BTreePageLeafHeader),
	Interior(BTreePageInteriorHeader),
}

struct BTreePageLeafHeader {
    type_flag: u8,
    freeblock_count: u16,
    cell_count: u16,
    cell_content_start: u16,
    fragmented_free_bytes: u8,
}

impl BTreePageLeafHeader {
    fn new(buf: &[u8]) -> BTreePageLeafHeader {
        let type_flag = buf[0];
        let freeblock_count = read_u16(buf, 1);
        let cell_count = read_u16(buf, 3);
        let cell_content_start = read_u16(buf, 5);
        let fragmented_free_bytes = buf[7];

        BTreePageLeafHeader {
            type_flag,
            freeblock_count,
            cell_count,
            cell_content_start,
            fragmented_free_bytes,
        }
    }
}

struct BTreePageInteriorHeader {
    type_flag: u8,
    freeblock_count: u16,
    cell_count: u16,
    cell_content_start: u16,
    fragmented_free_bytes: u8,
	right_most_pointer: u32,
}

impl BTreePageInteriorHeader {
    fn new(buf: &[u8]) -> BTreePageInteriorHeader {
        let type_flag = buf[0];
        let freeblock_count = read_u16(buf, 1);
        let cell_count = read_u16(buf, 3);
        let cell_content_start = read_u16(buf, 5);
        let fragmented_free_bytes = buf[7];
		let right_most_pointer: u32 = read_u32(buf, 8);
		println!("right most pointer {}", right_most_pointer);
        BTreePageInteriorHeader {
            type_flag,
            freeblock_count,
            cell_count,
            cell_content_start,
            fragmented_free_bytes,
			right_most_pointer,
        }
    }
}

enum Page {
	TableBTreeInteriorPage(TableBTreeInteriorPage),
	TableBTreeLeafPage(TableBTreeLeafPage),
}
struct TableBTreeInteriorPage {
	raw_buf: Vec<u8>,
	header: BTreePageInteriorHeader,
	cells: Vec<TableBTreeInteriorCell>,
}
struct TableBTreeLeafPage {
	raw_buf: Vec<u8>,
	header: BTreePageLeafHeader,
	cells: Vec<TableBTreeLeafCell>,
}

impl TableBTreeLeafPage {
	fn new(page: &[u8]) -> TableBTreeLeafPage{
		let raw_buf = Vec::from(page);
		let header = BTreePageLeafHeader::new(&page[0..8]);
		let mut cells: Vec<TableBTreeLeafCell> = Vec::new();
		for i in 0..header.cell_count {
			let offset = read_u16(&page, (8 + i * 2) as usize )as u16;
			// cell_pointer_array.push(read_u16(&page, (8 + i * 2) as usize ));
			// println!("cell poiner offset {}", offset);
			// println!("byte at offset {}", page[offset as usize]);
			let cell = TableBTreeLeafCell::new(&page, offset as usize);
			cells.push(cell);
			// eprintln!("{:x?}", cell.payload);
			// let record = Record::new(&cell.payload);
			// eprintln!("{:?}", record);
		}
		TableBTreeLeafPage { raw_buf, header, cells }
	}


}

impl TableBTreeInteriorPage {
	fn new(page: &[u8]) -> TableBTreeInteriorPage{
		let raw_buf = Vec::from(page);
		let header = BTreePageInteriorHeader::new(&page[0..12]);
		let mut cells: Vec<TableBTreeInteriorCell> = Vec::new();
		for i in 0..header.cell_count {
			let offset = read_u16(&page, (12 + i * 2) as usize )as u16;
			// cell_pointer_array.push(read_u16(&page, (8 + i * 2) as usize ));
			// println!("byte at offset {}", page[offset as usize]);
			let cell = TableBTreeInteriorCell::new(&page, offset as usize);
			// println!("cell {:?}", cell);
			cells.push(cell);
		}
		TableBTreeInteriorPage { raw_buf, header, cells }
	}
}

struct TableBTreeLeafCell {
	payload_size: u64,
	row_id: u64,
	payload: Vec<u8>,
	overflow_page: u64,
}

#[derive(Debug)]
struct TableBTreeInteriorCell {
	left_child_pointer: u32,
	row_id: u64,
}

#[derive(Debug)]
enum Column {
	NULL(NULL),
	I8(i8),
	I16(i16),
	I24(i32),
	I32(i32),
	I48(i64),
	I64(i64),
	F64(f64),
	False(bool),
	True(bool),
	Blob(Blob),
	Text(Text),
}
#[derive(Debug)]
struct NULL {

}
#[derive(Debug)]
struct Blob {
	size: u64,
	data: Vec<u8>,
}
#[derive(Debug)]
struct Text {
	size: u64,
	data: Vec<u8>,
}

#[derive(Debug)]
struct Record {
	header_size: u64,
	body: Vec<Column>,
}

impl Record {
	fn new(buf: &[u8]) -> Record{
		let mut offset = 0;
		let mut header_size: u64 = 0;
		let mut row_header: Vec<u64> = vec![];
		offset += read_var_int(buf, offset, &mut header_size);
		while offset < header_size as usize {
			let mut column_serial_type: u64 = 0;
			offset += read_var_int(buf, offset, &mut column_serial_type);
			row_header.push(column_serial_type);
		}
		let mut body: Vec<Column> = vec![];
		for entry in row_header {
			body.push(read_record_column(entry, buf, &mut offset));
		}
		// println!("offset after record {}",offset);
		Record { header_size, body}
	}
}

fn read_record_column(serial_type: u64, buf: &[u8], offset: &mut usize) -> Column {
	match serial_type {
		0 => Column::NULL(NULL{}),
		1 => {
			let size = 1;
			if buf.len() < *offset + size {
				Column::I8(0i8)
			} else {
				let val: i8 = u8::from_be(buf[*offset]) as i8;
				*offset += size;
				Column::I8(val)
			}
		},
		2 => {
			let size = 2;
			if buf.len() < *offset + size {
				Column::I16(0i16)
			} else {
				let mut byte_arr: [u8; 2] = [0u8; 2];
				byte_arr.copy_from_slice(&buf[*offset..*offset+size]);
				let val: i16 = u16::from_be_bytes(byte_arr) as i16;
				*offset += size;
				Column::I16(val)
			}
		},
		3 => {
			let size = 3;
			if buf.len() < *offset + size {
				Column::I24(0i32)
			} else {
				let mut byte_arr: [u8; 4] = [0u8; 4];
				byte_arr[1..].copy_from_slice(&buf[*offset..*offset+size]);
				let val: i32 = u32::from_be_bytes(byte_arr) as i32;
				//TODO: this cast will not result in the correct value
				*offset += size;
				Column::I24(val)
			}
		},
		4 => {
			let size = 4;
			if buf.len() < *offset + size {
				Column::I32(0i32)
			} else {
				let mut byte_arr: [u8; 4] = [0u8; 4];
				byte_arr.copy_from_slice(&buf[*offset..*offset+size]);
				let val: i32 = u32::from_be_bytes(byte_arr) as i32;
				*offset += size;
				Column::I32(val)
			}
		},
		5 => {
			let size = 6;
			if buf.len() < *offset + size {
				Column::I48(0i64)
			} else {
				let mut byte_arr: [u8; 8] = [0u8; 8];
				byte_arr[2..].copy_from_slice(&buf[*offset..*offset+size]);
				let val: i64 = u64::from_be_bytes(byte_arr) as i64;
				//TODO: this cast will not result in the correct value
				*offset += size;
				Column::I48(val)
			}
		},
		6 => {
			let size = 8;
			if buf.len() < *offset + size {
				Column::I64(0i64)
			} else {
				let mut byte_arr: [u8; 8] = [0u8; 8];
				byte_arr.copy_from_slice(&buf[*offset..*offset+size]);
				let val: i64 = u64::from_be_bytes(byte_arr) as i64;
				*offset += size;
				Column::I64(val)
			}
		},
		7 => {
			let size = 8;
			if buf.len() < *offset + size {
				Column::F64(0f64)
			} else {
				let mut byte_arr: [u8; 8] = [0u8; 8];
				byte_arr.copy_from_slice(&buf[*offset..*offset+size]);
				let val: f64 = f64::from_be_bytes(byte_arr);
				*offset += size;
				Column::F64(val)
			}
		},
		8 => Column::False(false),
		9 => Column::True(true),
		10 => panic!("unexpected record serial type 10"),
		11 => panic!("unexpected record serial type 11"),
		x => {
			if x%2==0 {
				let size = (x-12)/2;
				*offset += size as usize;
				Column::Blob(Blob{size, data: Vec::from(&buf[*offset-size as usize..*offset])})
			} else {
				let size = (x-13)/2;
				*offset += size as usize;
				let data = Vec::from(&buf[*offset-size as usize..*offset]);
				let str_data = String::from_utf8(data).unwrap();
				// println!("string data {}", str_data);
				Column::Text(Text{size, data: Vec::from(&buf[*offset-size as usize..*offset])})
			} 
		}
	}
}


impl TableBTreeLeafCell {
	fn new(page: &[u8], offset: usize) -> TableBTreeLeafCell{
		// let payload: Vec<u8> = vec![];
		let overflow_page: u64 = 0;
		let mut row_id: u64 = 0;
		let mut payload_size: u64 = 0;
		let mut offset = offset;
		offset += read_var_int(page, offset, &mut payload_size);
		offset += read_var_int(page, offset, &mut row_id);
		let payload: Vec<u8> = Vec::from(&page[offset..offset+payload_size as usize]);
		//TODO: handle overflow
		// println!("row_id {}   payload_size {} payload_size2 {}", row_id, payload_size, payload.len());
		TableBTreeLeafCell {payload_size, row_id, payload, overflow_page}
	}
}

impl TableBTreeInteriorCell {
	fn new(page: &[u8], offset: usize) -> TableBTreeInteriorCell{
		// let payload: Vec<u8> = vec![];
		let left_child_pointer: u32 = read_u32(page, offset);
		let mut row_id: u64 = 0;
		let mut offset = offset;
		offset += read_var_int(page, offset, &mut row_id);
		TableBTreeInteriorCell {left_child_pointer, row_id}
	}
}


fn main() {
    let mut f: File = std::fs::File::open(FILENAME).expect("failed to open file");
    parse_header(&mut f);
    read_first_page(&mut f);
	read_page(&mut f,2);
	let mut cells: Vec<TableBTreeLeafCell> = vec![];
	read_table(&mut f, 2, &mut cells);
    println!("Hello, world!");
	println!("Hello, world! {}", cells.len());
	let mut i = 0;
	for cell in cells {
		let record = Record::new(&cell.payload);
		if i % 1000 == 0 {
			println!("cell row_id {}, cell data {:?}", cell.row_id, record.body)
		}
		i += 1;
	}
	// let testval: [u8; 9] = [0b11000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000];
	// println!("{}", testval[0]);
	// let mut result: u64 = 0;
	// let length = read_var_int(&testval, 0, &mut result);
	// let x: u64 = 1 << 62;
	// println!("result {} {} {} {}", result, x, result-x, length);
}

fn parse_header(f: &mut File) {
    let mut buf = [0u8; HEADER_SIZE];
    std::io::Read::read(f, &mut buf).unwrap();
    SqliteHeader::new(&buf);
}

fn read_first_page(f: &mut File) {
    let mut page = [0u8; PAGE_SIZE - HEADER_SIZE];
    f.seek(std::io::SeekFrom::Start(HEADER_SIZE as u64))
        .expect("seek failed");
    std::io::Read::read(f, &mut page).unwrap();
	let h = BTreePageLeafHeader::new(&page[0..8]);
	
	eprintln!("{:x?}", page);
	println!("cell count {}", h.cell_count);
	// let mut cell_pointer_array:Vec<u16> = Vec::new();
	for i in 0..h.cell_count {
		let offset = read_u16(&page, (8 + i * 2) as usize ) - HEADER_SIZE as u16;
		// cell_pointer_array.push(read_u16(&page, (8 + i * 2) as usize ));
		println!("cell poiner offset {}", offset);
		println!("byte at offset {}", page[offset as usize]);
		let cell = TableBTreeLeafCell::new(&page, offset as usize);
		let record = Record::new(&cell.payload);
		eprintln!("{:x?}", record);
	}
}

fn read_page(f: &mut File, offset: usize) -> Page {
    let mut page = [0u8; PAGE_SIZE];
    f.seek(std::io::SeekFrom::Start((PAGE_SIZE*offset) as u64))
        .expect("seek failed");
    std::io::Read::read(f, &mut page).unwrap();
	let serial_type = page[0];
	// eprintln!("{:x?}", page);
	// eprintln!("serial type {}", serial_type);

	let parsed_page:Page = match serial_type {
		0x02 => panic!("not implemented interior index page 2"),
		0x05 => Page::TableBTreeInteriorPage(TableBTreeInteriorPage::new(&page)),
		0x0a => panic!("not implemented leaf index page 10"),
		0x0d => Page::TableBTreeLeafPage(TableBTreeLeafPage::new(&page)),
		_ => panic!("invalid page type"),
	};

	parsed_page
}

fn read_table(f: &mut File, root_page: u32, cells: &mut Vec<TableBTreeLeafCell>){
	let root = read_page(f, root_page as usize -1 );
	match root {
		Page::TableBTreeLeafPage(p) => {
			for cell in p.cells {
				cells.push(cell);
			}
		},
		Page::TableBTreeInteriorPage(p) => {
			println!("interior page {}", root_page);
			for cell in p.cells {
				read_table(f, cell.left_child_pointer, cells);
			}
			read_table(f, p.header.right_most_pointer, cells);
		},
	}
}