use std::cmp;
use std::io;
use std::fmt;
use std::error;
use std::result;
use std::os::fd;
use std::fs;
use std::rc;
use std::num;
use std::path;

fn is_zero(buf: &[u8]) -> bool {
  const ZZ: [u8; 8] = [0; 8];
  let misalign = buf.as_ptr() as usize % ZZ.len();
  if misalign != 0 && ZZ[misalign..] != buf[..ZZ.len() - misalign] {
    return false;
  }
  let buf = &buf[ZZ.len() - misalign..];
  if buf.len() < ZZ.len() {
    return &ZZ[..buf.len()] == buf;
  }
  let (buf, tail) = buf.split_at(buf.len() - buf.len() % ZZ.len());
  if ZZ != buf[..ZZ.len()] {
    return false;
  }
  buf[..buf.len() - ZZ.len()] == buf[ZZ.len()..] && &ZZ[..tail.len()] == tail
}

#[derive(Debug, Clone)]
enum SparseBlockContents {
  Hole(usize),
  Data(rc::Rc<Vec<u8>>),
}
impl SparseBlockContents {
  fn len(&self) -> usize {
    use SparseBlockContents::{Hole,Data};
    match self {
      Hole(sz) => *sz,
      Data(buf) => buf.len(),
    }
  }
}
impl cmp::PartialEq for SparseBlockContents {
  fn eq(&self, other: &Self) -> bool {
    assert_eq!(self.len(), other.len());
    use SparseBlockContents::{Hole,Data};
    match (self, other) {
      (Hole(_), Hole(_)) => true,
      (Hole(_), Data(buf)) | (Data(buf), Hole(_)) => is_zero(buf),
      (Data(lhb), Data(rhb)) => lhb == rhb,
    }
  }
}
#[derive(Debug, Clone)]
struct SparseBlock {
  off: isize,
  contents: SparseBlockContents,
}
impl SparseBlock {
  fn len(&self) -> usize {
    self.contents.len()
  }
}

#[derive(Debug, Clone, Copy)]
enum State {
  Hole(isize),
  Data(isize),
}
impl State {
  fn end(self) -> isize {
    match self {
      State::Hole(end) | State::Data(end) => end,
    }
  }
  fn whence(self) -> libc::c_int {
    match self {
      State::Hole(_) => libc::SEEK_HOLE,
      State::Data(_) => libc::SEEK_DATA,
    }
  }
}

#[derive(Debug)]
struct SparseFileIter {
  f: fs::File,
  bs: usize,
  off: isize,
  end: Option<isize>,
  state: State,
  buf: rc::Rc<Vec<u8>>,
  block: Option<SparseBlock>,
}
impl SparseFileIter {
  fn create(f: fs::File, bs: usize) -> io::Result<SparseFileIter> {
    let buf = rc::Rc::new(vec![0; bs]);
    let block = None;
    match lseek(&f, 0, libc::SEEK_DATA) {
      // This file is one big hole, and we have to find the end to know how big:
      Err(e) if e.raw_os_error() == Some(libc::ENXIO) => {
        let end = lseek(&f, 0, libc::SEEK_END)?;
        Ok(SparseFileIter{f, bs, off: 0, end: Some(end), state: State::Hole(end), buf, block})
      },
      // This is not a file or a block device, so we can't seek at all:
      Err(e) if e.raw_os_error() == Some(libc::ESPIPE) => Ok(SparseFileIter{f, bs, off: 0, end: None, state: State::Data(isize::MAX), buf, block}),
      // This is a block device, so we can seek but can't look for holes or data:
      Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
        let end = lseek(&f, 0, libc::SEEK_END)?;
        lseek(&f, 0, libc::SEEK_SET)?;
        Ok(SparseFileIter{f, bs, off: 0, end: Some(end), state: State::Data(end), buf, block})
      },
      // This is an error:
      Err(e) => Err(e),
      // The beginning is data, and we have to find a hole to know where it ends:
      Ok(0) => {
        let end = lseek(&f, 0, libc::SEEK_END)?;
        let hole = lseek(&f, 0, libc::SEEK_HOLE)?;
        lseek(&f, 0, libc::SEEK_SET)?;
        Ok(SparseFileIter{f, bs, off: 0, end: Some(end), state: State::Data(hole), buf, block})
      },
      // The beginning is a hole, and we have already found the data at its end:
      Ok(data) => {
        let end = lseek(&f, 0, libc::SEEK_END)?;
        lseek(&f, data - data % bs as isize, libc::SEEK_SET)?;
        Ok(SparseFileIter{f, bs, off: 0, end: Some(end), state: State::Hole(data), buf, block})
      },
    }
  }
  fn end(&self) -> Option<isize> {
    self.end
  }
}
impl<'b> fallible_streaming_iterator::FallibleStreamingIterator for SparseFileIter {
  type Item = SparseBlock;
  type Error = io::Error;
  fn advance(&mut self) -> Result<(), Self::Error> {
    use State::{Data, Hole};
    match self.state {
      Hole(end) if end < self.off + self.bs as isize => (),
      Data(_) => (),
      Hole(_) => {
        self.block = Some(SparseBlock{off: self.off, contents: SparseBlockContents::Hole(self.bs)});
        self.off += self.bs as isize;
        return Ok(());
      },
    };
    self.block = None;
    let mut buf_off = 0;
    loop {
      use std::io::Read;
      match self.f.read(&mut rc::Rc::get_mut(&mut self.buf).expect("somehow there's an outstanding reference to the buffer")[buf_off..]) {
        Ok(0) if buf_off == 0 => return Ok(()),
        Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "file size is not an even multiple of block size")),
        Ok(n) => {
          buf_off += n;
          if buf_off == self.bs {
            self.block = Some(SparseBlock{off: self.off, contents: SparseBlockContents::Data(rc::Rc::clone(&self.buf))});
            self.off += buf_off as isize;
            break;
          }
        },
        Err(e) if e.kind() != io::ErrorKind::Interrupted => return Err(e),
        Err(_) => continue,
      }
    }
    if self.end == Some(self.off) {
      return Ok(());
    }
    if self.state.end() <= self.off {
      self.state = match (self.state, lseek(&self.f, self.off, self.state.whence())) {
        (Data(_), Err(e)) if e.raw_os_error() == Some(libc::ENXIO) => {
          Hole(self.end.expect("seekable file doesn't have end set"))
        },
        (_, Err(e)) => return Err(e),
        (Data(_), Ok(end)) => {
          lseek(&self.f, end - end % self.bs as isize, libc::SEEK_SET)?;
          Hole(end)
        },
        (Hole(_), Ok(end)) => {
          lseek(&self.f, self.off, libc::SEEK_SET)?;
          Data(end)
        },
      }
    }
    Ok(())
  }
  fn get(&self) -> Option<&Self::Item> {
    self.block.as_ref()
  }
}

fn lseek(f: &fs::File, off: isize, whence: libc::c_int) -> io::Result<isize> {
  use fd::AsRawFd;
  match unsafe { libc::lseek(f.as_raw_fd(), off as libc::off_t, whence) } {
    -1 => Err(io::Error::last_os_error()),
    v => Ok(v as isize),
  }
}

fn fadvise(f: &fs::File, offset: isize, len: isize, advise: libc::c_int) -> io::Result<()> {
  use fd::AsRawFd;
  match unsafe { libc::posix_fadvise(f.as_raw_fd(), offset as libc::off_t, len as libc::off_t, advise) } {
    0 => Ok(()),
    e => Err(io::Error::from_raw_os_error(e)),
  }
}

const DEFAULT_LOGICAL_SECTOR_SIZE: usize = 512;
fn logical_sector_size(f: &fs::File) -> io::Result<usize> {
  use fd::AsRawFd;
  let mut ss: libc::c_int = 0;
  unsafe { io_block::os::linux::blksszget(f.as_raw_fd(), &mut ss).map_err(|e| { io::Error::from_raw_os_error(e as i32) })?; }
  if ss <= 0 {
    return Err(io::Error::other("device returned non-positive logical sector size"));
  }
  Ok(ss as usize)
}

#[derive(Debug)]
struct DiffFile {
  p: path::PathBuf,
  f: Option<fs::File>,
  bs: usize,
  cs: u32,
  exc: usize,
}
impl DiffFile {
  fn create(p: path::PathBuf, ss: usize, cs: u32) -> io::Result<DiffFile> {
    let bs = ss * cs as usize;
    Ok(DiffFile{p, f: None, bs, cs, exc: 0})
  }
  fn exception(&mut self, blk: &SparseBlock) -> io::Result<()> {
    if self.bs != blk.len() {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "data block size doesn't match diff file's block size"));
    }
    if self.exc + 1 > self.bs / 16 {
      return Err(io::Error::other("too many exceptions; try a larger cluster size"));
    }
    if self.f.is_none() {
      self.f = Some(fs::File::options().write(true).truncate(true).create(true).open(self.p.clone())?);
      let header: [u8; 16] = [b'S', b'n', b'A', b'p', 1, 0, 0, 0, 1, 0, 0, 0, (self.cs >> 0) as u8, (self.cs >> 8) as u8, (self.cs >> 16) as u8, (self.cs >> 24) as u8];
      use std::os::unix::fs::FileExt;
      self.f.as_ref().unwrap().write_all_at(&header, 0)?;
    }
    let f = self.f.as_ref().unwrap();
    let old = (blk.off as usize / self.bs) as u64;
    let new = 2 + self.exc as u64;
    let exc: [u8; 16] = [(old >> 0) as u8, (old >> 8) as u8, (old >> 16) as u8, (old >> 24) as u8,
                         (old >> 32) as u8, (old >> 40) as u8, (old >> 48) as u8, (old >> 56) as u8,
                         (new >> 0) as u8, (new >> 8) as u8, (new >> 16) as u8, (new >> 24) as u8,
                         (new >> 32) as u8, (new >> 40) as u8, (new >> 48) as u8, (new >> 56) as u8];
    use std::os::unix::fs::FileExt;
    f.write_all_at(&exc, (self.bs + 16 * self.exc) as u64)?;
    use SparseBlockContents::{Hole,Data};
    match &blk.contents {
      Hole(_) => f.set_len((self.bs as u64).checked_mul(new + 1).ok_or(io::Error::other("arithmetic overflow"))?)?,
      Data(buf) if is_zero(buf) => f.set_len((self.bs as u64).checked_mul(new + 1).ok_or(io::Error::other("arithmetic overflow"))?)?,
      Data(buf) => f.write_all_at(&buf, (self.bs as u64).checked_mul(new).ok_or(io::Error::other("arithmetic overflow"))?)?,
    }
    self.exc += 1;
    Ok(())
  }
  fn any_diffs(&self) -> bool {
    self.exc > 0
  }
}

#[derive(Debug, clap::Parser)]
#[command(version, about, long_about = None)]
struct Args {
  /// The origin file or device, which should be kept.  Read-only.
  #[arg(long)]
  orig: path::PathBuf,
  
  /// The destination file or device, which can be reproduced by combining the origin and the diff.  Read-only.
  #[arg(long)]
  dest: path::PathBuf,
  
  /// The file into which the diff will be written.  Will be overwritten.
  #[arg(long)]
  diff: path::PathBuf,
  
  /// The sector size.
  ///
  /// If unset (or 0) and the origin is a device, its logical sector size will be used.
  /// If unset (or 0) and the origin is a file, 512 is assumed.
  #[arg(long, default_value_t = 0, value_parser = parse_ss)]
  ss: usize,
  
  /// The cluster size, in sectors.
  ///
  /// Each cluster that differs is copied into the diff file, so smaller clusters can make for smaller diffs, however the total diff size is limited by the cluster size.
  #[arg(long, default_value_t = 2048, value_parser = parse_cs)]
  cs: u32,
}

#[derive(Debug, Clone)]
enum ArgError {
  Inner(num::ParseIntError),
  Msg(&'static str),
}
impl fmt::Display for ArgError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ArgError::Inner(e) => e.fmt(f),
      ArgError::Msg(s) => write!(f, "ArgError {}", s),
    }
  }
}
impl error::Error for ArgError {}

fn parse_ss(s: &str) -> Result<usize, ArgError> {
  if s.is_empty() {
    return Ok(0);
  }
  let n = if let Some(s) = s.strip_prefix("0x").or(s.strip_prefix("0X")).or(s.strip_prefix("+0x")).or(s.strip_prefix("+0X")) {
    if s.starts_with('+') {
      return Err(ArgError::Msg("unexpected + in --ss flag value"));
    }
    usize::from_str_radix(s, 16).map_err(|e| { ArgError::Inner(e) })?
  } else {
    usize::from_str_radix(s, 10).map_err(|e| { ArgError::Inner(e) })?
  };
  Ok(n)
}

fn parse_cs(s: &str) -> Result<u32, ArgError> {
  if s.is_empty() {
    return Err(ArgError::Msg("empty --cs flag value"));
  }
  let n = if let Some(s) = s.strip_prefix("0x").or(s.strip_prefix("0X")).or(s.strip_prefix("+0x")).or(s.strip_prefix("+0X")) {
    if s.starts_with('+') {
      return Err(ArgError::Msg("unexpected + in --cs flag value"));
    }
    u32::from_str_radix(s, 16).map_err(|e| { ArgError::Inner(e) })?
  } else {
    u32::from_str_radix(s, 10).map_err(|e| { ArgError::Inner(e) })?
  };
  if n == 0 {
    return Err(ArgError::Msg("--cs flag mustn't be 0"));
  }
  Ok(n)
}

#[derive(Debug, Clone)]
struct SameError;
impl fmt::Display for SameError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "files do not differ")
  }
}
impl error::Error for SameError {}

fn main() -> result::Result<(), Box<dyn error::Error>> {
  use clap::Parser;
  let args = Args::parse();
  let orig = fs::File::options().read(true).open(args.orig)?;
  let dest = fs::File::options().read(true).open(args.dest)?;
  match fadvise(&orig, 0, 0, libc::POSIX_FADV_SEQUENTIAL) {
    Ok(_) => (),
    Err(e) if e.raw_os_error() == Some(libc::ESPIPE) => (),
    Err(e) => return Err(Box::new(e)),
  }
  match fadvise(&dest, 0, 0, libc::POSIX_FADV_SEQUENTIAL) {
    Ok(_) => (),
    Err(e) if e.raw_os_error() == Some(libc::ESPIPE) => (),
    Err(e) => return Err(Box::new(e)),
  }
  let ss = match args.ss {
    0 => match logical_sector_size(&orig) {
      Ok(ss) => ss,
      Err(e) if e.raw_os_error() == Some(libc::ENOTTY) => DEFAULT_LOGICAL_SECTOR_SIZE,
      Err(e) => return Err(Box::new(e)),
    },
    ss => ss,
  };
  let cs = args.cs;
  if ss as u128 * cs as u128 > u64::MAX as u128 + 1 {
    return Err(Box::new(io::Error::other("block size is too large")));
  }
  let bs = ss * cs as usize;
  let mut orig = SparseFileIter::create(orig, bs)?;
  let mut dest = SparseFileIter::create(dest, bs)?;
  match (orig.end(), dest.end()) {
    (Some(orig), Some(dest)) if orig != dest => return Err(Box::new(io::Error::other("file sizes don't match"))),
    (Some(orig), _) if orig as usize % bs != 0 => return Err(Box::new(io::Error::other("origin file's size is not a multiple of chunk size"))),
    (_, Some(dest)) if dest as usize % bs != 0 => return Err(Box::new(io::Error::other("destination file's size is not a multiple of chunk size"))),
    _ => (),
  };
  let mut diff = DiffFile::create(args.diff, ss, cs)?;
  use fallible_streaming_iterator::FallibleStreamingIterator;
  loop {
    match (orig.next()?, dest.next()?) {
      (Some(orig), Some(dest)) => if orig.contents != dest.contents { diff.exception(dest)?; },
      (Some(_), None) => return Err(Box::new(io::Error::other("destination file ended before origin"))),
      (None, Some(_)) => return Err(Box::new(io::Error::other("origin file ended before destination"))),
      (None, None) => break,
    }
  }
  if !diff.any_diffs() {
    return Err(Box::new(SameError));
  }
  diff.f.as_ref().expect("there should be a file here by now").sync_all()?;
  Ok(())
}
