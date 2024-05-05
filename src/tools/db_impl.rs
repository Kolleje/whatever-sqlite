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

pub fn find_key_in_index(f: &mut File, root_page: u32, key: Column) -> Option<u64>{
	let root = read_page(f, root_page as usize);
	match root {
        Page::IndexBTreeLeafPage(p) => {
            for cell in p.cells {
				let index_record = Record::new(&cell.payload);
                let first = index_record.body.first().unwrap();
				let last = index_record.body.last().unwrap();
				if *first == key {
					println!("found key in leaf, page {} row_id {:?}", root_page, *last);
					return Some(force_cast_column_to_u64(last));
				}
            }
			None
        }
        Page::IndexBTreeInteriorPage(p) => {
            // println!("interior page {}", root_page);
            for cell in p.cells {
				let index_record = Record::new(&cell.payload);
				let first = index_record.body.first().unwrap();
				let last = index_record.body.last().unwrap();
				if key < *first {
					return find_key_in_index(f, cell.left_child_pointer, key);
				}
				if key == *first {
					println!("found key in interior, page {} row_id {:?}", root_page, *last);
					return Some(force_cast_column_to_u64(last));
				}
            }
            return find_key_in_index(f, p.header.right_most_pointer, key);
        }
		_ => { panic!("expected index page, found table page"); }
    }
}

fn force_cast_column_to_u64(c: &Column) -> u64{
	match *c {
		Column::I8(v)	=> v as u64,
		Column::I16(v)	=> v as u64,
		Column::I24(v)	=> v as u64,
		Column::I32(v)	=> v as u64,
		Column::I48(v)	=> v as u64,
		Column::I64(v)	=> v as u64,
		Column::True(_)	=> 1,
		Column::False(_)	=> 0,
		_ => panic!("not an int"),
	}
}