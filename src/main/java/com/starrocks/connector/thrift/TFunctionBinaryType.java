/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.thrift;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2022-10-12")
public enum TFunctionBinaryType implements org.apache.thrift.TEnum {
  BUILTIN(0),
  HIVE(1),
  NATIVE(2),
  IR(3);

  private final int value;

  private TFunctionBinaryType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static TFunctionBinaryType findByValue(int value) { 
    switch (value) {
      case 0:
        return BUILTIN;
      case 1:
        return HIVE;
      case 2:
        return NATIVE;
      case 3:
        return IR;
      default:
        return null;
    }
  }
}
