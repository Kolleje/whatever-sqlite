// pub mod helper;

pub fn read_u16(buf: &[u8], start: usize) -> u16 {
    let mut byte_arr: [u8; 2] = [0u8; 2];
    byte_arr.copy_from_slice(&buf[start..start+2]);
    u16::from_be_bytes(byte_arr)
}

pub fn read_u32(buf: &[u8], start: usize) -> u32 {
    let mut byte_arr: [u8; 4] = [0u8; 4];
    byte_arr.copy_from_slice(&buf[start..start+4]);
    u32::from_be_bytes(byte_arr)
}


//     pub fn parse_u64(buf: &[u8], start: usize, len: usize) -> u64 {
//     let mut byte_arr: [u8; 8] = [0u8; 8];
//     byte_arr[8 - len..].copy_from_slice(&buf[start..start + len]);
//     u64::from_be_bytes(byte_arr)
// }


pub fn read_var_int(buf: &[u8], offset: usize, result: &mut u64) -> usize{
	let mask: u8 = 0b10000000;
	let mask2: u8 = 0b01111111;
	let max_len = 9;
	// let mut arr: [u8; 8] = [0u8; 8];
	*result = 0;
	let mut next_exists = true;
	let mut i = 0;
	while next_exists && i < max_len {
		if i > 0 {
			*result = *result << 7;
		}
		let current = if i == max_len - 1 { buf[offset+i] } else { buf[offset+i] & mask2 };
		*result = *result | (current as u64); 
		next_exists = mask & buf[offset+i] == mask;
		i += 1;
	}
	i
}

