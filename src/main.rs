// use std::{fs::File, io::Read};

use std::{fs::File, io::Seek};

use tools::defs::*;
// use tools::helper::{read_u16, read_u32, read_var_int};
use tools::db_impl::*;
pub mod tools;

const FILENAME: &str = "./mostbasic3.sqlite";



fn main() {
    let mut f: File = std::fs::File::open(FILENAME).expect("failed to open file");
    parse_header(&mut f);
    read_first_page(&mut f);
    read_page(&mut f, 2);
    let mut cells: Vec<TableBTreeLeafCell> = vec![];
    // read_table(&mut f, 2, &mut cells);
    println!("Hello, world!");
    // println!("Hello, world! {}", cells.len());
    // let mut i = 0;
    // for cell in cells {
    //     let record = Record::new(&cell.payload);
    //     if i % 1000 == 0 {
    //     	println!("cell row_id {}, cell data {:?}", cell.row_id, record.body)
    //     }
    //     i += 1;
    // }
	let key = 5;
	let res = find_by_primary_key(&mut f, 2, key);
	if let Some(c) = res {
		Record::new(&c.payload).print();
	} else {
		println!("not found key = {}", key);
	}

	for i in 2..10 {
		let key = Column::I64(i);
		let x = find_key_in_index(&mut f, 3225, key);
		match x {
			Some(_) => {},
			None => {
				println!("did not find in index for value= {}", i);
				panic!("aaaaaaaaaaaaaaaaaaaaaa")
			}
		}
	}
    // read_page(&mut f, 3224);
    // let testval: [u8; 9] = [0b11000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000, 0b10000000];
    // println!("{}", testval[0]);
    // let mut result: u64 = 0;
    // let length = read_var_int(&testval, 0, &mut result);
    // let x: u64 = 1 << 62;
    // println!("result {} {} {} {}", result, x, result-x, length);
}

