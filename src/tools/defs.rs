use crate::tools::helper::{read_u16, read_u32, read_var_int};
use std::cmp::{self, Ordering};

pub const HEADER_SIZE: usize = 100;
pub const PAGE_SIZE: usize = 4096;
const SQLITE_HEADER_STRING: &str = "SQLite format 3\0";

//######################################################
//sqlite header
//######################################################

pub struct SqliteHeader {
    version_string: String,
    page_size: u16,
    // version_valid_for: u64,
    version: u32,
}

impl SqliteHeader {
    pub fn new(buf: &[u8; 100]) -> SqliteHeader {
        let page_size = read_u16(buf, &mut 16);
        let version = read_u32(buf, &mut 96);
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

//######################################################
// pages
//######################################################

pub enum Page {
    TableBTreeLeafPage(TableBTreeLeafPage),
    TableBTreeInteriorPage(TableBTreeInteriorPage),
	IndexBTreeLeafPage(IndexBTreeLeafPage),
	IndexBTreeInteriorPage(IndexBTreeInteriorPage),
}

//######################################################

pub struct TableBTreeLeafPage {
    raw_buf: Vec<u8>,
    pub header: BTreePageLeafHeader,
    pub cells: Vec<TableBTreeLeafCell>,
}

impl TableBTreeLeafPage {
    pub fn new(page: &[u8]) -> TableBTreeLeafPage {
        let raw_buf = Vec::from(page);
        let header = BTreePageLeafHeader::new(&page[0..8]);
        let mut cells: Vec<TableBTreeLeafCell> = Vec::new();
        for i in 0..header.cell_count {
            let offset = read_u16(&page, &mut((8 + i * 2) as usize)) as u16;
            let cell = TableBTreeLeafCell::new(&page, offset as usize);
            cells.push(cell);
        }
        TableBTreeLeafPage {
            raw_buf,
            header,
            cells,
        }
    }
}

//######################################################

pub struct TableBTreeInteriorPage {
    raw_buf: Vec<u8>,
    pub header: BTreePageInteriorHeader,
    pub cells: Vec<TableBTreeInteriorCell>,
}

impl TableBTreeInteriorPage {
    pub fn new(page: &[u8]) -> TableBTreeInteriorPage {
        let raw_buf = Vec::from(page);
        let header = BTreePageInteriorHeader::new(&page[0..12]);
        let mut cells: Vec<TableBTreeInteriorCell> = Vec::new();
        for i in 0..header.cell_count {
            let offset = read_u16(&page, &mut ((12 + i * 2) as usize)) as u16;
            let cell = TableBTreeInteriorCell::new(&page, offset as usize);
            cells.push(cell);
        }
        TableBTreeInteriorPage {
            raw_buf,
            header,
            cells,
        }
    }
}

//######################################################

pub struct IndexBTreeLeafPage {
    raw_buf: Vec<u8>,
    pub header: BTreePageLeafHeader,
    pub cells: Vec<IndexBTreeLeafCell>,
}

impl IndexBTreeLeafPage {
    pub fn new(page: &[u8]) -> IndexBTreeLeafPage {
        let raw_buf = Vec::from(page);
        let header = BTreePageLeafHeader::new(&page[0..8]);
        let mut cells: Vec<IndexBTreeLeafCell> = Vec::new();
        for i in 0..header.cell_count {
            let offset = read_u16(&page, &mut((8 + i * 2) as usize)) as u16;
            let cell = IndexBTreeLeafCell::new(&page, offset as usize);
            cells.push(cell);
        }
        IndexBTreeLeafPage {
            raw_buf,
            header,
            cells,
        }
    }
}

//######################################################

pub struct IndexBTreeInteriorPage {
    raw_buf: Vec<u8>,
    pub header: BTreePageInteriorHeader,
    pub cells: Vec<IndexBTreeInteriorCell>,
}


impl IndexBTreeInteriorPage {
    pub fn new(page: &[u8]) -> IndexBTreeInteriorPage {
        let raw_buf = Vec::from(page);
        let header = BTreePageInteriorHeader::new(&page[0..12]);
        let mut cells: Vec<IndexBTreeInteriorCell> = Vec::new();
        for i in 0..header.cell_count {
            let offset = read_u16(&page, &mut((12 + i * 2) as usize)) as u16;
            let cell = IndexBTreeInteriorCell::new(&page, offset as usize);
            cells.push(cell);
        }
        IndexBTreeInteriorPage {
            raw_buf,
            header,
            cells,
        }
    }
}


//######################################################
// page header
//######################################################

pub enum BTreeHeader {
    Leaf(BTreePageLeafHeader),
    Interior(BTreePageInteriorHeader),
}

//######################################################

pub struct BTreePageLeafHeader {
    pub type_flag: u8,
    pub freeblock_count: u16,
    pub cell_count: u16,
    pub cell_content_start: u16,
    pub fragmented_free_bytes: u8,
}

impl BTreePageLeafHeader {
    pub fn new(buf: &[u8]) -> BTreePageLeafHeader {
        let type_flag = buf[0];
        let freeblock_count = read_u16(buf, &mut 1);
        let cell_count = read_u16(buf, &mut 3);
        let cell_content_start = read_u16(buf, &mut 5);
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

//######################################################

pub struct BTreePageInteriorHeader {
    pub type_flag: u8,
    pub freeblock_count: u16,
    pub cell_count: u16,
    pub cell_content_start: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: u32,
}

impl BTreePageInteriorHeader {
    pub fn new(buf: &[u8]) -> BTreePageInteriorHeader {
        let type_flag = buf[0];
        let freeblock_count = read_u16(buf, &mut 1);
        let cell_count = read_u16(buf, &mut 3);
        let cell_content_start = read_u16(buf, &mut 5);
        let fragmented_free_bytes = buf[7];
        let right_most_pointer: u32 = read_u32(buf, &mut 8);
        // println!("right most pointer {}", right_most_pointer);
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

//######################################################
// cell 
//######################################################

#[derive(Debug)]
pub struct TableBTreeLeafCell {
    pub payload_size: u64,
    pub row_id: u64,
    pub payload: Vec<u8>,
    pub overflow_page: u32,
}

impl TableBTreeLeafCell {
    pub fn new(page: &[u8], offset: usize) -> TableBTreeLeafCell {
        // let payload: Vec<u8> = vec![];
        let overflow_page: u32 = 0;

        let mut payload_size: u64 = 0;
        let mut offset = offset;
        offset += read_var_int(page, offset, &mut payload_size);

        let mut row_id: u64 = 0;
        offset += read_var_int(page, offset, &mut row_id);

        let payload: Vec<u8> = Vec::from(&page[offset..offset + payload_size as usize]);
        //TODO: handle overflow
        // println!("row_id {}   payload_size {} payload_size2 {}", row_id, payload_size, payload.len());
        TableBTreeLeafCell {
            payload_size,
            row_id,
            payload,
            overflow_page,
        }
    }
}

//######################################################

#[derive(Debug)]
pub struct TableBTreeInteriorCell {
    pub left_child_pointer: u32,
    pub row_id: u64,
}

impl TableBTreeInteriorCell {
    pub fn new(page: &[u8], offset: usize) -> TableBTreeInteriorCell {
		// let payload: Vec<u8> = vec![];
        let mut offset = offset;
        let left_child_pointer: u32 = read_u32(page, &mut offset);

        let mut row_id: u64 = 0;
        offset += read_var_int(page, offset, &mut row_id);
        TableBTreeInteriorCell {
            left_child_pointer,
            row_id,
        }
    }
}

//######################################################

pub struct IndexBTreeLeafCell {
    pub payload_size: u64,
    pub payload: Vec<u8>,
    pub overflow_page: u32,
}

impl IndexBTreeLeafCell {
    pub fn new(page: &[u8], offset: usize) -> IndexBTreeLeafCell {
        let mut offset = offset;
        let mut payload_size: u64 = 0;
        offset += read_var_int(page, offset, &mut payload_size);

        let payload: Vec<u8> = Vec::from(&page[offset..offset + payload_size as usize]);
		offset += payload_size as usize;

        let overflow_page: u32 = 0;

        IndexBTreeLeafCell {
            payload_size,
            payload,
            overflow_page,
        }
    }
}

//######################################################

pub struct IndexBTreeInteriorCell {
	pub left_child_pointer: u32,
    pub payload_size: u64,
    pub payload: Vec<u8>,
    pub overflow_page: u32,
}

impl IndexBTreeInteriorCell {
    pub fn new(page: &[u8], offset: usize) -> IndexBTreeInteriorCell {
        let mut offset = offset;
        let left_child_pointer: u32 = read_u32(page,&mut offset);

        let mut payload_size: u64 = 0;
        offset += read_var_int(page, offset, &mut payload_size);

        let payload: Vec<u8> = Vec::from(&page[offset..offset + payload_size as usize]);
        let overflow_page: u32 = 0;

        IndexBTreeInteriorCell {
			left_child_pointer,
            payload_size,
            payload,
            overflow_page,
        }
    }
}

//######################################################
// record
//######################################################

#[derive(Debug)]
pub struct Record {
    pub header_size: u64,
    pub body: Vec<Column>,
}

impl Record {
	pub fn new(buf: &[u8]) -> Record {
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
        Record { header_size, body }
    }

    pub fn print(&self) {
        for c in &self.body {
            match c {
                Column::Blob(b) => {
                    println!("blob {}", String::from_utf8(b.data.clone()).unwrap())
                }
                Column::Text(b) => {
                    println!("blob {}", String::from_utf8(b.data.clone()).unwrap())
                }
                x => {
                    println!("{:?}", x);
                }
            }
        }
    }
}

//######################################################
// column
//######################################################

#[derive(Debug)]
pub enum Column {
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
pub struct NULL {}
#[derive(Debug)]
struct Blob {
    pub size: u64,
    pub data: Vec<u8>,
}
#[derive(Debug)]
pub struct Text {
    pub size: u64,
    pub data: Vec<u8>,
}

pub fn read_record_column(serial_type: u64, buf: &[u8], offset: &mut usize) -> Column {
    match serial_type {
        0 => Column::NULL(NULL {}),
        1 => {
            let size = 1;
            if buf.len() < *offset + size {
                Column::I8(0i8)
            } else {
                let val: i8 = u8::from_be(buf[*offset]) as i8;
                *offset += size;
                Column::I8(val)
            }
        }
        2 => {
            let size = 2;
            if buf.len() < *offset + size {
                Column::I16(0i16)
            } else {
                let mut byte_arr: [u8; 2] = [0u8; 2];
                byte_arr.copy_from_slice(&buf[*offset..*offset + size]);
                let val: i16 = u16::from_be_bytes(byte_arr) as i16;
                *offset += size;
                Column::I16(val)
            }
        }
        3 => {
            let size = 3;
            if buf.len() < *offset + size {
                Column::I24(0i32)
            } else {
                let mut byte_arr: [u8; 4] = [0u8; 4];
                byte_arr[1..].copy_from_slice(&buf[*offset..*offset + size]);
                let val: i32 = u32::from_be_bytes(byte_arr) as i32;
                //TODO: this cast will not result in the correct value
                *offset += size;
                Column::I24(val)
            }
        }
        4 => {
            let size = 4;
            if buf.len() < *offset + size {
                Column::I32(0i32)
            } else {
                let mut byte_arr: [u8; 4] = [0u8; 4];
                byte_arr.copy_from_slice(&buf[*offset..*offset + size]);
                let val: i32 = u32::from_be_bytes(byte_arr) as i32;
                *offset += size;
                Column::I32(val)
            }
        }
        5 => {
            let size = 6;
            if buf.len() < *offset + size {
                Column::I48(0i64)
            } else {
                let mut byte_arr: [u8; 8] = [0u8; 8];
                byte_arr[2..].copy_from_slice(&buf[*offset..*offset + size]);
                let val: i64 = u64::from_be_bytes(byte_arr) as i64;
                //TODO: this cast will not result in the correct value
                *offset += size;
                Column::I48(val)
            }
        }
        6 => {
            let size = 8;
            if buf.len() < *offset + size {
                Column::I64(0i64)
            } else {
                let mut byte_arr: [u8; 8] = [0u8; 8];
                byte_arr.copy_from_slice(&buf[*offset..*offset + size]);
                let val: i64 = u64::from_be_bytes(byte_arr) as i64;
                *offset += size;
                Column::I64(val)
            }
        }
        7 => {
            let size = 8;
            if buf.len() < *offset + size {
                Column::F64(0f64)
            } else {
                let mut byte_arr: [u8; 8] = [0u8; 8];
                byte_arr.copy_from_slice(&buf[*offset..*offset + size]);
                let val: f64 = f64::from_be_bytes(byte_arr);
                *offset += size;
                Column::F64(val)
            }
        }
        8 => Column::False(false),
        9 => Column::True(true),
        10 => panic!("unexpected record serial type 10"),
        11 => panic!("unexpected record serial type 11"),
        x => {
            if x % 2 == 0 {
                let size = (x - 12) / 2;
                *offset += size as usize;
                Column::Blob(Blob {
                    size,
                    data: Vec::from(&buf[*offset - size as usize..*offset]),
                })
            } else {
                let size = (x - 13) / 2;
                *offset += size as usize;
                let data = Vec::from(&buf[*offset - size as usize..*offset]);
                let str_data = String::from_utf8(data).unwrap();
                // println!("string data {}", str_data);
                Column::Text(Text {
                    size,
                    data: Vec::from(&buf[*offset - size as usize..*offset]),
                })
            }
        }
    }
}

impl cmp::Eq for Column {

} 

impl cmp::PartialEq for Column {
	fn eq(&self, other: &Self) -> bool {
		match self {
			Column::NULL(_) => {
				match other {
					Column::NULL(_) => true,
					_ => false,
				}
			},
			Column::False(_) => {
				match other {
					Column::False(_) => true,
					_ => false,
				}
			},
			Column::True(_) => {
				match other {
					Column::True(_) => true,
					_ => false,
				}
			},
			Column::Text(t1) => {
				match other {
					Column::Text(t2) => true, // TODO impl eq for text
					_ => false,
				}
			},
			Column::Blob(b1) => {
				match other {
					Column::Text(b2) => true, // TODO impl eq for Blob
					_ => false,
				}
			},
			Column::F64(s) => {
				match other {
					Column::F64(o) => *s == *o,
					Column::I8(o) => *s == *o as f64,
					Column::I16(o) => *s == *o as f64,
					Column::I24(o) => *s == *o as f64,
					Column::I32(o) => *s == *o as f64,
					Column::I48(o) => *s == *o as f64,
					Column::I64(o) => *s == *o as f64,
					_ => false,
				}
			}
			Column::I8(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o,
					Column::I16(o) => *s as i16 == *o,
					Column::I24(o) => *s as i32 == *o,
					Column::I32(o) => *s as i32 == *o,
					Column::I48(o) => *s as i64 == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			Column::I16(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o as i16,
					Column::I16(o) => *s == *o,
					Column::I24(o) => *s as i32 == *o,
					Column::I32(o) => *s as i32 == *o,
					Column::I48(o) => *s as i64 == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			Column::I24(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o as i32,
					Column::I16(o) => *s == *o as i32,
					Column::I24(o) => *s == *o,
					Column::I32(o) => *s as i32 == *o,
					Column::I48(o) => *s as i64 == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			Column::I32(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o as i32,
					Column::I16(o) => *s == *o as i32,
					Column::I24(o) => *s == *o,
					Column::I32(o) => *s as i32 == *o,
					Column::I48(o) => *s as i64 == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			Column::I48(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o as i64,
					Column::I16(o) => *s == *o as i64,
					Column::I24(o) => *s == *o as i64,
					Column::I32(o) => *s == *o as i64,
					Column::I48(o) => *s == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			Column::I64(s) => {
				match other {
					Column::NULL(_) => false,
					Column::Blob(_) => false,
					Column::Text(_) => false,
					Column::True(_) => false,
					Column::False(_) => false,
					Column::F64(o) => *s as f64 == *o,
					Column::I8(o) => *s == *o as i64,
					Column::I16(o) => *s == *o as i64,
					Column::I24(o) => *s == *o as i64,
					Column::I32(o) => *s == *o as i64,
					Column::I48(o) => *s == *o,
					Column::I64(o) => *s as i64 == *o,
				}
			},
			// _ => panic!("aaaaa"),
		}	
	}
}

//maybe implement partial only?
impl cmp::Ord for Column {
	fn cmp(&self, other: &Column) -> Ordering{
		match self {
			Column::NULL(_) => {
				match other {
					Column::NULL(_) => Ordering::Equal,
					_ => Ordering::Less,
				}
			},
			Column::True(_) => {
				panic!("no ordering for true")
			},
			Column::False(_) => {
				panic!("no ordering for false")
			},
			Column::Text(s) => {
				panic!("not implemnted yet: ordering text")
			},
			Column::Blob(s) => {
				panic!("not implemnted yet: ordering blob")
			},
			s => {
				match other {
					Column::True(_) => panic!("no ordering for true right side"),
					Column::False(_) => panic!("no ordering for false right side"),
					Column::NULL(_) => Ordering::Greater,
					Column::F64(o) => force_cast_column_to_f64(s).partial_cmp(o).unwrap(),
					Column::Text(_) => Ordering::Less,
					Column::Blob(_) => Ordering::Less,
					o => force_cast_column_to_i64(s).cmp(&force_cast_column_to_i64(o)),
				}
			},
		}
	}
}

impl cmp::PartialOrd for Column {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

fn force_cast_column_to_i64(c: &Column) -> i64{
	match *c {
		Column::I8(v)	=> v as i64,
		Column::I16(v)	=> v as i64,
		Column::I24(v)	=> v as i64,
		Column::I32(v)	=> v as i64,
		Column::I48(v)	=> v as i64,
		Column::I64(v)	=> v as i64,
		_ => panic!("can not cast to int"),
	}
}

fn force_cast_column_to_f64(c: &Column) -> f64{
	match *c {
		Column::I8(v)	=> v as f64,
		Column::I16(v)	=> v as f64,
		Column::I24(v)	=> v as f64,
		Column::I32(v)	=> v as f64,
		Column::I48(v)	=> v as f64,
		Column::I64(v)	=> v as f64,
		_ => panic!("can not cast to float"),
	}
}