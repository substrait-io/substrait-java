package io.substrait.isthmus.api;

public class ExampleCalciteReflectiveSchema {

  class VehicleTest {
    public int test_id;
    public int vehicle_id;
    public String test_date;
    public String test_type;
    public String test_class;
    public String test_result;
    public int test_mileage;
    public String postcode_area;
  }

  class Vehicle {
    public int vehicle_id;

    public String make;
    public String model;
    public String colour;
    public String fuel_type;
    public int cylinder_capacity;
    public String first_use_date;
  }

  public VehicleTest[] tests = {};
  public Vehicle[] vehicles = {};
}
