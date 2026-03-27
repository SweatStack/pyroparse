//! Zero-knowledge FIT binary reader.
//!
//! Reads the FIT file framing format (headers, definitions, data messages)
//! without any knowledge of the FIT profile. Yields [`FitEvent`]s that a
//! higher-level decoder can interpret using profile definitions.
//!
//! # Design
//!
//! - **Zero-copy**: data message field bytes are slices into the input buffer.
//! - **Minimal allocation**: definitions are small structs stored in a fixed
//!   16-slot array (one per local message type, per FIT spec).
//! - **Chained files**: handles concatenated FIT files by yielding
//!   [`FitEvent::FileHeader`] and [`FitEvent::Crc`] at boundaries.
//! - **CRC**: computed incrementally. Reported in [`FitEvent::Crc`] for the
//!   caller to decide whether to fail or warn.

// ---------------------------------------------------------------------------
// CRC-16
// ---------------------------------------------------------------------------

const CRC_TABLE: [u16; 16] = [
    0x0000, 0xCC01, 0xD801, 0x1400, 0xF001, 0x3C00, 0x2800, 0xE401,
    0xA001, 0x6C00, 0x7800, 0xB401, 0x5000, 0x9C01, 0x8801, 0x4400,
];

/// Compute CRC-16 for a byte slice, starting from an initial value.
fn crc16(init: u16, data: &[u8]) -> u16 {
    data.iter().fold(init, |crc, &byte| {
        // Lower nibble.
        let tmp = CRC_TABLE[(crc & 0xF) as usize];
        let crc = (crc >> 4) & 0x0FFF;
        let crc = crc ^ tmp ^ CRC_TABLE[(byte & 0xF) as usize];
        // Upper nibble.
        let tmp = CRC_TABLE[(crc & 0xF) as usize];
        let crc = (crc >> 4) & 0x0FFF;
        crc ^ tmp ^ CRC_TABLE[((byte >> 4) & 0xF) as usize]
    })
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Layout of a single field within a definition message.
#[derive(Debug, Clone)]
pub struct FieldLayout {
    /// Field definition number (maps to the FIT profile).
    pub number: u8,
    /// Size in bytes in data messages.
    pub size: u8,
    /// Raw base type byte from the definition (mask with `& 0x1F` for type ID).
    pub base_type: u8,
}

/// Layout of a single developer field within a definition message.
#[derive(Debug, Clone)]
pub struct DevFieldLayout {
    /// Developer-defined field number.
    pub number: u8,
    /// Size in bytes in data messages.
    pub size: u8,
    /// Identifies which developer (CIQ app) owns this field.
    pub dev_data_index: u8,
}

/// Information about a parsed file header.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileHeader {
    pub header_size: u8,
    pub protocol_version: u8,
    pub profile_version: u16,
    pub data_size: u32,
}

/// A parsed definition for a local message type.
#[derive(Debug, Clone)]
pub struct MessageDef {
    /// Global FIT message number (e.g. 20 = Record, 18 = Session).
    pub global_message_number: u16,
    /// Byte order for multi-byte fields.
    pub big_endian: bool,
    /// Regular field definitions (in definition order).
    pub fields: Vec<FieldLayout>,
    /// Developer field definitions (in definition order).
    pub dev_fields: Vec<DevFieldLayout>,
    /// Total size of regular field data in bytes.
    pub data_size: usize,
    /// Total size of developer field data in bytes.
    pub dev_data_size: usize,
}

impl MessageDef {
    /// Total bytes in a data message (regular + developer fields).
    pub fn total_data_size(&self) -> usize {
        self.data_size + self.dev_data_size
    }
}

/// Events yielded by [`FitReader`] as it walks through a FIT file.
///
/// Data events carry zero-copy byte slices into the input buffer. The
/// definition for a data message can be retrieved via
/// [`FitReader::def(local)`] after the event is yielded.
#[derive(Debug)]
pub enum FitEvent<'a> {
    /// A file header was parsed. For chained files, this appears multiple
    /// times. The decoder should reset state when it sees this after a Crc.
    #[allow(dead_code)]
    FileHeader(FileHeader),

    /// A definition message was parsed. The reader stores it internally.
    /// Use [`FitReader::def(local)`] to inspect it.
    Definition {
        local: u8,
        global_message_number: u16,
    },

    /// A normal data message. `field_bytes` and `dev_field_bytes` are
    /// zero-copy slices into the input buffer.
    Data {
        local: u8,
        field_bytes: &'a [u8],
        dev_field_bytes: &'a [u8],
    },

    /// A compressed-timestamp data message. Same as Data but includes the
    /// 5-bit time offset from the record header.
    CompressedData {
        local: u8,
        time_offset: u8,
        field_bytes: &'a [u8],
        dev_field_bytes: &'a [u8],
    },

    /// End of a FIT file section. `valid` indicates whether the CRC matched.
    /// For chained files, another FileHeader may follow.
    Crc {
        #[allow(dead_code)]
        valid: bool,
    },
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the binary reader.
#[derive(Debug, Clone)]
pub struct FitError {
    pub message: String,
    pub offset: usize,
}

impl FitError {
    fn new(offset: usize, msg: impl Into<String>) -> Self {
        Self { message: msg.into(), offset }
    }
}

impl std::fmt::Display for FitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FIT parse error at offset 0x{:X}: {}", self.offset, self.message)
    }
}

impl std::error::Error for FitError {}

// ---------------------------------------------------------------------------
// FitReader
// ---------------------------------------------------------------------------

/// Zero-knowledge FIT binary reader.
///
/// Walks through a FIT file (or chained files) and yields [`FitEvent`]s.
/// Knows nothing about message semantics — that's the decoder's job.
///
/// # Usage
///
/// ```ignore
/// let mut reader = FitReader::new(bytes)?;
/// while let Some(event) = reader.next()? {
///     match event {
///         FitEvent::Data { def, field_bytes, .. } => { /* decode */ }
///         FitEvent::Crc { valid } => { /* reset state */ }
///         _ => {}
///     }
/// }
/// ```
pub struct FitReader<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Byte position where the current FIT section's data records end.
    data_end: usize,
    /// Running CRC for the current FIT section.
    crc: u16,
    /// Whether the header bytes were included in the CRC
    /// (true when header has no CRC of its own).
    header_in_crc: bool,
    /// Local message type definitions (0–15).
    defs: [Option<MessageDef>; 16],
    /// True if we've yielded at least one FileHeader.
    started: bool,
}

impl<'a> FitReader<'a> {
    /// Get the current definition for a local message type.
    ///
    /// Call this after receiving a [`FitEvent::Data`] or
    /// [`FitEvent::CompressedData`] to access the message's field layout.
    pub fn def(&self, local: u8) -> Option<&MessageDef> {
        self.defs[local as usize].as_ref()
    }

    /// Create a new reader for the given bytes.
    ///
    /// Returns an error if the bytes are too short to contain a valid FIT
    /// header. Does NOT read the header yet — call [`next()`] to start.
    pub fn new(buf: &'a [u8]) -> Result<Self, FitError> {
        if buf.len() < 12 {
            return Err(FitError::new(0, "file too short for FIT header"));
        }
        Ok(Self {
            buf,
            pos: 0,
            data_end: 0,
            crc: 0,
            header_in_crc: false,
            defs: Default::default(),
            started: false,
        })
    }

    /// Yield the next event, or `None` when the file is fully consumed.
    pub fn next(&mut self) -> Result<Option<FitEvent<'a>>, FitError> {
        // --- Need to parse a header? ---
        if !self.started || (self.pos > self.data_end && self.pos < self.buf.len()) {
            if self.pos >= self.buf.len() {
                return Ok(None);
            }
            return self.read_file_header().map(Some);
        }

        // --- At the CRC boundary? ---
        if self.pos == self.data_end {
            return self.read_crc().map(Some);
        }

        // --- Past everything? ---
        if self.pos >= self.buf.len() {
            return Ok(None);
        }

        // --- Parse a record (definition or data message) ---
        self.read_record()
    }

    // -- File header ---------------------------------------------------------

    fn read_file_header(&mut self) -> Result<FitEvent<'a>, FitError> {
        let start = self.pos;
        let header_size = self.read_u8()? as usize;

        if start + header_size > self.buf.len() {
            return Err(FitError::new(start, "file header extends past end of file"));
        }
        if header_size < 12 {
            return Err(FitError::new(start, "header size < 12"));
        }

        let protocol_version = self.read_u8()?;
        let profile_version = u16::from_le_bytes([self.read_u8()?, self.read_u8()?]);
        let data_size = u32::from_le_bytes([
            self.read_u8()?, self.read_u8()?, self.read_u8()?, self.read_u8()?,
        ]);

        // Validate ".FIT" signature.
        let sig = self.read_slice(4)?;
        if sig != b".FIT" {
            return Err(FitError::new(start + 8, "missing .FIT signature"));
        }

        // Header CRC (14-byte headers only).
        let mut has_header_crc = false;
        if header_size >= 14 {
            let crc_bytes = self.read_slice(2)?;
            let header_crc = u16::from_le_bytes([crc_bytes[0], crc_bytes[1]]);
            if header_crc != 0 {
                has_header_crc = true;
                let computed = crc16(0, &self.buf[start..start + 12]);
                if computed != header_crc {
                    return Err(FitError::new(start + 12, "header CRC mismatch"));
                }
            }
        }

        // Skip any extra header bytes (future-proofing: header_size may grow).
        if self.pos < start + header_size {
            self.pos = start + header_size;
        }

        // Set up section boundaries and CRC state.
        self.data_end = start + header_size + data_size as usize;
        self.crc = 0;
        self.header_in_crc = !has_header_crc;
        self.started = true;

        // If header bytes are included in data CRC, seed it now.
        if self.header_in_crc {
            self.crc = crc16(0, &self.buf[start..start + header_size]);
        }

        // Reset definitions for this FIT section.
        self.defs = Default::default();

        Ok(FitEvent::FileHeader(FileHeader {
            header_size: header_size as u8,
            protocol_version,
            profile_version,
            data_size,
        }))
    }

    // -- CRC -----------------------------------------------------------------

    fn read_crc(&mut self) -> Result<FitEvent<'a>, FitError> {
        if self.pos + 2 > self.buf.len() {
            return Err(FitError::new(self.pos, "file truncated at CRC"));
        }
        let crc_bytes = &self.buf[self.pos..self.pos + 2];
        let file_crc = u16::from_le_bytes([crc_bytes[0], crc_bytes[1]]);
        self.pos += 2;

        let valid = file_crc == self.crc;
        Ok(FitEvent::Crc { valid })
    }

    // -- Records (definition + data messages) --------------------------------

    fn read_record(&mut self) -> Result<Option<FitEvent<'a>>, FitError> {
        let record_start = self.pos;
        let header_byte = self.read_u8()?;

        if header_byte & 0x80 != 0 {
            // Compressed timestamp data message.
            let local = (header_byte >> 5) & 0x03;
            let time_offset = header_byte & 0x1F;
            self.read_data_message(record_start, local, Some(time_offset))
        } else if header_byte & 0x40 != 0 {
            // Definition message.
            let has_dev = header_byte & 0x20 != 0;
            let local = header_byte & 0x0F;
            self.read_definition(record_start, local, has_dev)
        } else {
            // Normal data message.
            let local = header_byte & 0x0F;
            self.read_data_message(record_start, local, None)
        }
    }

    fn read_definition(
        &mut self,
        record_start: usize,
        local: u8,
        has_dev: bool,
    ) -> Result<Option<FitEvent<'a>>, FitError> {
        let _reserved = self.read_u8()?;
        let arch = self.read_u8()?;
        let big_endian = arch == 1;

        let gm_bytes = self.read_slice(2)?;
        let global_message_number = if big_endian {
            u16::from_be_bytes([gm_bytes[0], gm_bytes[1]])
        } else {
            u16::from_le_bytes([gm_bytes[0], gm_bytes[1]])
        };

        let num_fields = self.read_u8()? as usize;
        let mut fields = Vec::with_capacity(num_fields);
        let mut data_size = 0usize;
        for _ in 0..num_fields {
            let number = self.read_u8()?;
            let size = self.read_u8()?;
            let base_type = self.read_u8()?;
            data_size += size as usize;
            fields.push(FieldLayout { number, size, base_type });
        }

        let mut dev_fields = Vec::new();
        let mut dev_data_size = 0usize;
        if has_dev {
            let num_dev = self.read_u8()? as usize;
            dev_fields.reserve(num_dev);
            for _ in 0..num_dev {
                let number = self.read_u8()?;
                let size = self.read_u8()?;
                let dev_data_index = self.read_u8()?;
                dev_data_size += size as usize;
                dev_fields.push(DevFieldLayout { number, size, dev_data_index });
            }
        }

        // Update CRC for the entire record (header byte + content).
        self.crc = crc16(self.crc, &self.buf[record_start..self.pos]);

        // Store the definition.
        self.defs[local as usize] = Some(MessageDef {
            global_message_number,
            big_endian,
            fields,
            dev_fields,
            data_size,
            dev_data_size,
        });

        Ok(Some(FitEvent::Definition { local, global_message_number }))
    }

    fn read_data_message(
        &mut self,
        record_start: usize,
        local: u8,
        time_offset: Option<u8>,
    ) -> Result<Option<FitEvent<'a>>, FitError> {
        let def = self.defs[local as usize]
            .as_ref()
            .ok_or_else(|| FitError::new(
                record_start,
                format!("data message for local type {} without preceding definition", local),
            ))?;

        let total = def.total_data_size();
        if self.pos + total > self.buf.len() {
            return Err(FitError::new(
                self.pos,
                format!("data message truncated: need {} bytes, {} available", total, self.buf.len() - self.pos),
            ));
        }

        let data_size = def.data_size;
        let field_bytes = &self.buf[self.pos..self.pos + data_size];
        let dev_field_bytes = &self.buf[self.pos + data_size..self.pos + total];
        self.pos += total;

        // Update CRC for the entire record (header byte + field data).
        self.crc = crc16(self.crc, &self.buf[record_start..self.pos]);

        let event = match time_offset {
            Some(offset) => FitEvent::CompressedData {
                local,
                time_offset: offset,
                field_bytes,
                dev_field_bytes,
            },
            None => FitEvent::Data {
                local,
                field_bytes,
                dev_field_bytes,
            },
        };

        Ok(Some(event))
    }

    // -- Low-level byte reading ----------------------------------------------

    #[inline]
    fn read_u8(&mut self) -> Result<u8, FitError> {
        if self.pos >= self.buf.len() {
            return Err(FitError::new(self.pos, "unexpected end of data"));
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    fn read_slice(&mut self, n: usize) -> Result<&'a [u8], FitError> {
        if self.pos + n > self.buf.len() {
            return Err(FitError::new(self.pos, "unexpected end of data"));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid FIT file from parts.
    fn build_fit_file(records: &[u8]) -> Vec<u8> {
        let data_size = records.len() as u32;
        let mut buf = Vec::new();

        // 14-byte header (with header CRC = 0 → included in data CRC).
        buf.push(14);                                          // header_size
        buf.push(0x20);                                        // protocol version 2.0
        buf.extend_from_slice(&100u16.to_le_bytes());          // profile version 1.0
        buf.extend_from_slice(&data_size.to_le_bytes());       // data_size
        buf.extend_from_slice(b".FIT");                        // signature
        buf.extend_from_slice(&0u16.to_le_bytes());            // header CRC = 0

        // Data records.
        let data_start = buf.len();
        buf.extend_from_slice(records);

        // Compute CRC over header + data (since header CRC = 0).
        let crc = crc16(0, &buf[0..data_start + records.len()]);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Build a definition message for a given global message number.
    fn definition_record(local: u8, global: u16, fields: &[(u8, u8, u8)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x40 | (local & 0x0F));  // definition header
        buf.push(0);                       // reserved
        buf.push(0);                       // architecture: little-endian
        buf.extend_from_slice(&global.to_le_bytes());
        buf.push(fields.len() as u8);
        for &(num, size, base_type) in fields {
            buf.push(num);
            buf.push(size);
            buf.push(base_type);
        }
        buf
    }

    /// Build a data message with raw field bytes.
    fn data_record(local: u8, field_data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(local & 0x0F);  // normal data header
        buf.extend_from_slice(field_data);
        buf
    }

    /// Build a compressed-timestamp data message.
    fn compressed_data_record(local: u8, time_offset: u8, field_data: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x80 | ((local & 0x03) << 5) | (time_offset & 0x1F));
        buf.extend_from_slice(field_data);
        buf
    }

    #[test]
    fn test_minimal_file() {
        // Definition for local 0 = global 20 (record) with one uint8 field.
        let mut records = definition_record(0, 20, &[(3, 1, 0x02)]);
        records.extend(data_record(0, &[140]));  // heart_rate = 140

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        // FileHeader
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::FileHeader(_)));

        // Definition
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Definition { local, global_message_number } => {
                assert_eq!(*local, 0);
                assert_eq!(*global_message_number, 20);
                let def = reader.def(0).unwrap();
                assert_eq!(def.fields.len(), 1);
                assert_eq!(def.fields[0].number, 3);
                assert_eq!(def.fields[0].size, 1);
            }
            _ => panic!("expected Definition, got {:?}", event),
        }

        // Data
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, field_bytes, .. } => {
                assert_eq!(*local, 0);
                let def = reader.def(0).unwrap();
                assert_eq!(def.global_message_number, 20);
                assert_eq!(*field_bytes, &[140]);
            }
            _ => panic!("expected Data, got {:?}", event),
        }

        // CRC
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Crc { valid: true }));

        // EOF
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn test_compressed_timestamp() {
        let mut records = definition_record(0, 20, &[(3, 1, 0x02)]);
        records.extend(compressed_data_record(0, 27, &[155]));

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader
        reader.next().unwrap(); // Definition

        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::CompressedData { local, time_offset, field_bytes, .. } => {
                assert_eq!(*local, 0);
                assert_eq!(*time_offset, 27);
                assert_eq!(*field_bytes, &[155]);
            }
            _ => panic!("expected CompressedData, got {:?}", event),
        }
    }

    #[test]
    fn test_multiple_local_types() {
        // Local 0 = record (1 field), local 1 = session (1 field).
        let mut records = Vec::new();
        records.extend(definition_record(0, 20, &[(3, 1, 0x02)]));   // record: heart_rate
        records.extend(definition_record(1, 18, &[(5, 1, 0x00)]));   // session: sport
        records.extend(data_record(0, &[130]));  // record data
        records.extend(data_record(1, &[2]));    // session data (cycling)
        records.extend(data_record(0, &[135]));  // another record

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader
        let event = reader.next().unwrap().unwrap(); // Def local=0
        assert!(matches!(event, FitEvent::Definition { local: 0, global_message_number: 20 }));
        let event = reader.next().unwrap().unwrap(); // Def local=1
        assert!(matches!(event, FitEvent::Definition { local: 1, global_message_number: 18 }));

        // First record data.
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, field_bytes, .. } => {
                assert_eq!(*local, 0);
                assert_eq!(reader.def(0).unwrap().global_message_number, 20);
                assert_eq!(*field_bytes, &[130]);
            }
            _ => panic!("expected record Data"),
        }

        // Session data.
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, field_bytes, .. } => {
                assert_eq!(*local, 1);
                assert_eq!(reader.def(1).unwrap().global_message_number, 18);
                assert_eq!(*field_bytes, &[2]);
            }
            _ => panic!("expected session Data"),
        }

        // Second record data.
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, field_bytes, .. } => {
                assert_eq!(*local, 0);
                assert_eq!(reader.def(0).unwrap().global_message_number, 20);
                assert_eq!(*field_bytes, &[135]);
            }
            _ => panic!("expected record Data"),
        }
    }

    #[test]
    fn test_developer_fields() {
        // Definition with developer data flag set.
        let mut def_bytes = Vec::new();
        def_bytes.push(0x60);  // definition + developer data flag, local 0
        def_bytes.push(0);     // reserved
        def_bytes.push(0);     // little-endian
        def_bytes.extend_from_slice(&20u16.to_le_bytes()); // global = record
        def_bytes.push(1);     // 1 regular field
        def_bytes.extend_from_slice(&[3, 1, 0x02]); // heart_rate: uint8
        def_bytes.push(1);     // 1 developer field
        def_bytes.extend_from_slice(&[0, 2, 0]); // field 0, 2 bytes, dev index 0

        let mut records = def_bytes;
        records.extend(data_record(0, &[140, 0x34, 0x12])); // HR + 2 dev bytes

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader

        // Definition with dev fields.
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Definition { local, .. } => {
                let def = reader.def(*local).unwrap();
                assert_eq!(def.fields.len(), 1);
                assert_eq!(def.dev_fields.len(), 1);
                assert_eq!(def.dev_fields[0].size, 2);
                assert_eq!(def.data_size, 1);
                assert_eq!(def.dev_data_size, 2);
            }
            _ => panic!("expected Definition"),
        }

        // Data with dev field bytes split correctly.
        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { field_bytes, dev_field_bytes, .. } => {
                assert_eq!(*field_bytes, &[140]);
                assert_eq!(*dev_field_bytes, &[0x34, 0x12]);
            }
            _ => panic!("expected Data"),
        }
    }

    #[test]
    fn test_chained_files() {
        // Two FIT files concatenated.
        let records1 = definition_record(0, 20, &[(3, 1, 0x02)]);
        let records2 = definition_record(0, 18, &[(5, 1, 0x00)]);

        let mut buf = build_fit_file(&records1);
        buf.extend(build_fit_file(&records2));

        let mut reader = FitReader::new(&buf).unwrap();

        // First file.
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::FileHeader(_)));
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Definition { global_message_number: 20, .. }));
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Crc { valid: true }));

        // Second file.
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::FileHeader(_)));
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Definition { global_message_number: 18, .. }));
        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Crc { valid: true }));

        // EOF.
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn test_local_type_redefinition() {
        // Define local 0 as record, use it, redefine as session, use it.
        let mut records = Vec::new();
        records.extend(definition_record(0, 20, &[(3, 1, 0x02)]));
        records.extend(data_record(0, &[140]));
        records.extend(definition_record(0, 18, &[(5, 1, 0x00)]));
        records.extend(data_record(0, &[2]));

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader
        reader.next().unwrap(); // Def: record

        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, .. } => {
                assert_eq!(reader.def(*local).unwrap().global_message_number, 20);
            }
            _ => panic!("expected record Data"),
        }

        reader.next().unwrap(); // Def: session (redefines local 0)

        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, .. } => {
                assert_eq!(reader.def(*local).unwrap().global_message_number, 18);
            }
            _ => panic!("expected session Data"),
        }
    }

    #[test]
    fn test_crc_mismatch() {
        // Build a file with a definition + one data message, then corrupt
        // a data byte so the CRC won't match.
        let mut records = definition_record(0, 20, &[(3, 1, 0x02)]);
        records.extend(data_record(0, &[140]));
        let mut buf = build_fit_file(&records);

        // Corrupt the data message payload (last byte before CRC).
        let crc_pos = buf.len() - 2;
        buf[crc_pos - 1] ^= 0xFF;

        let mut reader = FitReader::new(&buf).unwrap();
        reader.next().unwrap(); // FileHeader
        reader.next().unwrap(); // Definition
        reader.next().unwrap(); // Data (corrupted)

        let event = reader.next().unwrap().unwrap();
        assert!(matches!(event, FitEvent::Crc { valid: false }));
    }

    #[test]
    fn test_missing_signature() {
        let mut buf = vec![0u8; 14];
        buf[0] = 14; // header_size
        // No .FIT signature at bytes 8-11.

        let mut reader = FitReader::new(&buf).unwrap();
        let result = reader.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains(".FIT signature"));
    }

    #[test]
    fn test_data_without_definition() {
        let records = data_record(0, &[140]);
        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader

        let result = reader.next();
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("without preceding definition"));
    }

    #[test]
    fn test_multi_field_data() {
        // Record with 3 fields: heart_rate(1), power(2), speed(2).
        let mut records = definition_record(0, 20, &[
            (3, 1, 0x02),   // heart_rate: uint8
            (7, 2, 0x84),   // power: uint16
            (6, 2, 0x84),   // speed: uint16
        ]);
        records.extend(data_record(0, &[
            150,             // heart_rate
            0x2C, 0x01,      // power = 300 (LE)
            0xE8, 0x03,      // speed = 1000 (LE)
        ]));

        let buf = build_fit_file(&records);
        let mut reader = FitReader::new(&buf).unwrap();

        reader.next().unwrap(); // FileHeader
        reader.next().unwrap(); // Definition

        let event = reader.next().unwrap().unwrap();
        match &event {
            FitEvent::Data { local, field_bytes, .. } => {
                let def = reader.def(*local).unwrap();
                assert_eq!(field_bytes.len(), 5); // 1 + 2 + 2
                assert_eq!(def.fields.len(), 3);

                // Verify we can slice individual fields from the flat bytes.
                let mut offset = 0;
                for f in &def.fields {
                    let end = offset + f.size as usize;
                    let _field_data = &field_bytes[offset..end];
                    offset = end;
                }
                assert_eq!(offset, 5);
            }
            _ => panic!("expected Data"),
        }
    }

    #[test]
    fn test_real_fit_file() {
        // Parse the actual test fixture to verify against real-world data.
        let path = std::path::Path::new("tests/fixtures/test.fit");
        if !path.exists() {
            return; // skip if fixtures not available
        }
        let buf = std::fs::read(path).unwrap();
        let mut reader = FitReader::new(&buf).unwrap();

        let mut headers = 0;
        let mut definitions = 0;
        let mut data_messages = 0;
        let mut compressed = 0;
        let mut crcs = 0;

        while let Some(event) = reader.next().unwrap() {
            match event {
                FitEvent::FileHeader(_) => headers += 1,
                FitEvent::Definition { .. } => definitions += 1,
                FitEvent::Data { .. } => data_messages += 1,
                FitEvent::CompressedData { .. } => compressed += 1,
                FitEvent::Crc { valid } => {
                    crcs += 1;
                    assert!(valid, "CRC mismatch in test.fit");
                }
            }
        }

        assert_eq!(headers, 1, "expected 1 file header");
        assert!(definitions > 0, "expected at least 1 definition");
        assert!(data_messages + compressed > 0, "expected data messages");
        assert_eq!(crcs, 1, "expected 1 CRC");
    }

    #[test]
    fn test_multi_session_fit_file() {
        let path = std::path::Path::new("tests/fixtures/cycling-rowing-cycling-rowing.fit");
        if !path.exists() {
            return;
        }
        let buf = std::fs::read(path).unwrap();
        let mut reader = FitReader::new(&buf).unwrap();

        let mut headers = 0;
        let mut crcs = 0;

        while let Some(event) = reader.next().unwrap() {
            match event {
                FitEvent::FileHeader(_) => headers += 1,
                FitEvent::Crc { valid } => {
                    crcs += 1;
                    assert!(valid, "CRC mismatch in multi-session file");
                }
                _ => {}
            }
        }

        // Multi-session file: should have exactly 1 header and 1 CRC
        // (it's a single FIT file with multiple sessions, not chained).
        assert!(headers >= 1);
        assert_eq!(headers, crcs);
    }

    #[test]
    fn test_developer_fields_fit_file() {
        let path = std::path::Path::new("tests/fixtures/with-developer-fields.fit");
        if !path.exists() {
            return;
        }
        let buf = std::fs::read(path).unwrap();
        let mut reader = FitReader::new(&buf).unwrap();

        let mut has_dev_fields = false;

        while let Some(event) = reader.next().unwrap() {
            if let FitEvent::Definition { local, .. } = event {
                if let Some(def) = reader.def(local) {
                    if !def.dev_fields.is_empty() {
                        has_dev_fields = true;
                    }
                }
            }
        }

        assert!(has_dev_fields, "expected developer fields in this fixture");
    }

    #[test]
    fn test_crc16_known_values() {
        // Verify CRC against known values from the FIT SDK.
        assert_eq!(crc16(0, &[]), 0);
        assert_eq!(crc16(0, &[0x00]), 0x0000);

        // The CRC should be deterministic and match the lookup table approach.
        let data = b".FIT";
        let crc = crc16(0, data);
        assert_ne!(crc, 0, "CRC of .FIT should be non-zero");

        // Verify incremental == batch.
        let batch = crc16(0, b"hello world");
        let incremental = crc16(crc16(0, b"hello"), b" world");
        assert_eq!(batch, incremental);
    }
}
