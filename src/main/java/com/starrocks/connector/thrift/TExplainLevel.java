/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.thrift;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2022-10-12")
public enum TExplainLevel implements org.apache.thrift.TEnum {
  NORMAL(0),
  VERBOSE(1);

  private final int value;

  private TExplainLevel(int value) {
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
  public static TExplainLevel findByValue(int value) { 
    switch (value) {
      case 0:
        return NORMAL;
      case 1:
        return VERBOSE;
      default:
        return null;
    }
  }
}