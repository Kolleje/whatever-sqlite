use crate::tools::{defs::*, helper::read_u16};
use std::{fs::File, io::Seek};

pub fn parse_header(f: &mut File) {
    let mut buf = [0u8; HEADER_SIZE];
    std::io::Read::read(f, &mut buf).unwrap();
    SqliteHeader::new(&buf);
}

pub fn read_first_page(f: &mut File) {
    let mut page = [0u8; PAGE_SIZE - HEADER_SIZE];
    f.seek(std::io::SeekFrom::Start(HEADER_SIZE as u64))
        .expect("seek failed");
    std::io::Read::read(f, &mut page).unwrap();
    let h = BTreePageLeafHeader::new(&page[0..8]);

    eprintln!("{:x?}", page);
    println!("cell count {}", h.cell_count);
    // let mut cell_pointer_array:Vec<u16> = Vec::new();
    for i in 0..h.cell_count {
        let offset = read_u16(&page, &mut ((8 + i * 2) as usize)) - HEADER_SIZE as u16;
        // cell_pointer_array.push(read_u16(&page, (8 + i * 2) as usize ));
        let cell = TableBTreeLeafCell::new(&page, offset as usize);
        let record = Record::new(&cell.payload);
        record.print();
        // eprintln!("{:?}", record);
    }
}

pub fn read_page(f: &mut File, page: usize) -> Page {
	if page == 0 {
		panic!("read_page offset == 0");
	}
	let offset = page - 1;
    let mut page = [0u8; PAGE_SIZE];
    f.seek(std::io::SeekFrom::Start((PAGE_SIZE * offset) as u64))
        .expect("seek failed");
    std::io::Read::read(f, &mut page).unwrap();
    let serial_type = page[0];
    // eprintln!("serial type {}", serial_type);
	
    let parsed_page: Page = match serial_type {
		0x02 => Page::IndexBTreeInteriorPage(IndexBTreeInteriorPage::new(&page)),
        0x05 => Page::TableBTreeInteriorPage(TableBTreeInteriorPage::new(&page)),
		0x0a => Page::IndexBTreeLeafPage(IndexBTreeLeafPage::new(&page)),
        0x0d => Page::TableBTreeLeafPage(TableBTreeLeafPage::new(&page)),
        _ => panic!("invalid page type"),
    };

	// if offset == 1  {
	// 	if let Page::TableBTreeInteriorPage(p) = &parsed_page {
	// 		for cell in &p.cells {
	// 			eprintln!("{:?}", cell);	
	// 		}
	// 	}
	// 	// eprintln!("{:x?}", page);
		
	// }

    parsed_page
}

pub fn read_table(f: &mut File, root_page: u32, cells: &mut Vec<TableBTreeLeafCell>) {
    let root = read_page(f, root_page as usize);
    match root {
        Page::TableBTreeLeafPage(p) => {
            for cell in p.cells {
                cells.push(cell);
            }
        }
        Page::TableBTreeInteriorPage(p) => {
            println!("interior page {}", root_page);
            for cell in p.cells {
                read_table(f, cell.left_child_pointer, cells);
            }
            read_table(f, p.header.right_most_pointer, cells);
        }
		_ => { panic!("expected table page, found index page"); }
    }
}

pub fn find_by_primary_key(f: &mut File, root_page: u32, key: u64) -> Option<TableBTreeLeafCell>{
	let root = read_page(f, root_page as usize);
    match root {
        Page::TableBTreeLeafPage(p) => {
            for cell in p.cells {
                if cell.row_id == key {
					return Some(cell)
				} 
            }
			None
        }
        Page::TableBTreeInteriorPage(p) => {
            println!("interior page {}", root_page);
            for cell in p.cells {
				if key <= cell.row_id {
					return find_by_primary_key(f, cell.left_child_pointer, key);
				}
            }
            return find_by_primary_key(f, p.header.right_most_pointer, key);
        }
		_ => { panic!("expected table page, found index page"); }
    }
}

pub fn find_keys_in_index(f: &mut File, root_page: u32, key: Column) -> Option<Vec<u64>>{
	let mut result: Vec<u64> = vec![];
	find_key_in_index_impl(f, root_page, &key, &mut result, false);
	if result.len() > 0 {
		Some(result)
	} else {
		None
	}
}

pub fn find_key_in_index(f: &mut File, root_page: u32, key: Column) -> Option<u64>{
	let mut result: Vec<u64> = vec![];
	find_key_in_index_impl(f, root_page, &key, &mut result, true);
	if result.len() > 0 {
		Some(result[0])
	} else {
		None
	}
}

fn find_key_in_index_impl(f: &mut File, root_page: u32, key: &Column, result:&mut Vec<u64>, distinct: bool){
	let root = read_page(f, root_page as usize);
	match root {
        Page::IndexBTreeLeafPage(p) => {
            for cell in p.cells {
				let index_record = Record::new(&cell.payload);
                let first = index_record.body.first().unwrap();
				let last = index_record.body.last().unwrap();
				if *first == *key {
					// println!("found key in leaf, page {} row_id {:?}, record len= {}", root_page, *last, index_record.body.len());
					result.push(force_cast_column_to_u64(last));
					if distinct {
						return;
					}
				}
            }
        }
        Page::IndexBTreeInteriorPage(p) => {
            // println!("interior page {}", root_page);
            for cell in p.cells {
				let index_record = Record::new(&cell.payload);
				let first = index_record.body.first().unwrap();
				let last = index_record.body.last().unwrap();
				if *key < *first {
					println!("deeper into tree {:?}", *first);
					return find_key_in_index_impl(f, cell.left_child_pointer, key, result, distinct);
				}
				if *key == *first {
					println!("found key in interior, page {} row_id {:?}", root_page, *last);
					result.push(force_cast_column_to_u64(last));
					if distinct {
						return;
					}
					find_key_in_index_impl(f, cell.left_child_pointer, key, result, distinct);
				}
            }
            return find_key_in_index_impl(f, p.header.right_most_pointer, key, result, distinct);
        }
		_ => { panic!("expected index page, found table page"); }
    }
}

fn force_cast_column_to_u64(c: &Column) -> u64{
	match *c {
		Column::I64(v)	=> v as u64,
		Column::True	=> 1,
		Column::False	=> 0,
		_ => panic!("not an int"),
	}
}

pub fn find_by_primary_key_list(f: &mut File, root_page: u32, key_list: &Vec<u64>) -> Option<Vec<TableBTreeLeafCell>>{
	if key_list.len() == 0 {
		return None;
	}
	let mut results: Vec<TableBTreeLeafCell> = vec![];
	let mut key_list_sorted = key_list.clone();
	let mut current_index = 0;
	key_list_sorted.sort();
	_find_by_primary_key_list(f, root_page, &key_list_sorted, &mut current_index, &mut results);

	if results.len() == 0 {
		None
	} else {
		Some(results)
	}
}

fn _find_by_primary_key_list(f: &mut File, root_page: u32, key_list_sorted: &Vec<u64>, current_index: &mut usize, results: &mut Vec<TableBTreeLeafCell>){
	let root = read_page(f, root_page as usize);
    match root {
        Page::TableBTreeLeafPage(p) => {
			let mut current_key = key_list_sorted[*current_index];
            for cell in p.cells {
				if cell.row_id < current_key {
					continue;
				}
                if cell.row_id == current_key {
					results.push(cell);
				}
				*current_index += 1;
				if key_list_sorted.len() <= *current_index {
					return;
				}
				current_key = key_list_sorted[*current_index];
            }
        }
        Page::TableBTreeInteriorPage(p) => {
            println!("interior page {}", root_page);
			let mut current_key = key_list_sorted[*current_index];
            for cell in p.cells {
				if current_key <= cell.row_id {
					_find_by_primary_key_list(f, cell.left_child_pointer, key_list_sorted, current_index, results);
					// return find_by_primary_key(f, cell.left_child_pointer, key);
					if key_list_sorted.len() <= *current_index {
						return;
					}
					current_key = key_list_sorted[*current_index];
				}
            }
			_find_by_primary_key_list(f, p.header.right_most_pointer, key_list_sorted, current_index, results);
            // find_by_primary_key(f, p.header.right_most_pointer, key);
        }
		_ => { panic!("expected table page, found index page"); }
    }
}

// fn table_find_rows_with_key_set(f: &mut File, root_page: u32, key: &Column, result:&mut Vec<u64>){
// 	Option<TableBTreeLeafCell>
// }