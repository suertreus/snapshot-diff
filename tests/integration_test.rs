#[cfg(test)]
mod tests {
  use std::error;
  use std::process;

  // This test writes to a file
  #[test]
  fn integration_test() -> Result<(), Box<dyn error::Error>> {
    assert!(process::Command::new("tests/integration_test").status()?.success());
    Ok(())
  }
}
