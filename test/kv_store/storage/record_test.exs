defmodule KVStore.Storage.RecordTest do
  use ExUnit.Case
  doctest KVStore.Storage.Record

  test "creates valid records" do
    key = "test_key"
    value = "test_value"

    record_data = KVStore.Storage.Record.create(key, value)
    assert is_list(record_data)
    # header, key, value
    assert length(record_data) == 3

    # Check that the record can be read back
    # Create a temporary file to test reading
    temp_file = Path.join(System.tmp_dir!(), "test_record.tmp")

    try do
      {:ok, file} = :file.open(temp_file, [:raw, :binary, :write])
      :file.write(file, IO.iodata_to_binary(record_data))
      :file.close(file)

      # Read the record back
      {:ok, file} = :file.open(temp_file, [:raw, :binary, :read])
      {:ok, record} = KVStore.Storage.Record.read(file, 0)
      :file.close(file)

      assert record.key == key
      assert record.value == value
      assert record.is_tombstone == false
      assert record.magic == 0xCAFEBABE
      assert record.version == 0x01
    after
      File.rm(temp_file)
    end
  end

  test "creates valid tombstone records" do
    key = "test_key"

    record_data = KVStore.Storage.Record.create_tombstone(key)
    assert is_list(record_data)
    # header, key (no value)
    assert length(record_data) == 2

    # Check that the tombstone can be read back
    temp_file = Path.join(System.tmp_dir!(), "test_tombstone.tmp")

    try do
      {:ok, file} = :file.open(temp_file, [:raw, :binary, :write])
      :file.write(file, IO.iodata_to_binary(record_data))
      :file.close(file)

      # Read the record back
      {:ok, file} = :file.open(temp_file, [:raw, :binary, :read])
      {:ok, record} = KVStore.Storage.Record.read(file, 0)
      :file.close(file)

      assert record.key == key
      assert record.value == ""
      assert record.is_tombstone == true
      assert record.magic == 0xCAFEBABE
      assert record.version == 0x01
    after
      File.rm(temp_file)
    end
  end

  test "calculates correct record sizes" do
    key = "test_key"
    value = "test_value"

    size = KVStore.Storage.Record.size(key, value)
    expected_size = KVStore.Storage.Record.header_size() + byte_size(key) + byte_size(value)
    assert size == expected_size
  end

  test "handles different data types" do
    # Test with binary data
    binary_key = <<1, 2, 3, 4>>
    binary_value = <<5, 6, 7, 8>>

    record_data = KVStore.Storage.Record.create(binary_key, binary_value)
    assert is_list(record_data)

    # Test with atom
    atom_key = :test_atom
    atom_value = :test_value

    record_data = KVStore.Storage.Record.create(atom_key, atom_value)
    assert is_list(record_data)
  end
end
