import java.time.*;
import java.util.*;

public class Try1 {

  public static void main(String[] args) {

    ParkingFloor floor = new ParkingFloor(
        1,
        List.of(
            new SmallSpot("S1"),
            new MediumSpot("M1"),
            new LargeSpot("L1")));

    ParkingLot lot = new ParkingLot(
        "LOT-1",
        List.of(floor),
        new NearestSpotStrategy(),
        new HourlyPricingStrategy());

    Vehicle car = new Vehicle(
        "DL01AB1234",
        VehicleType.CAR);

    ParkingTicket ticket = lot.park(car);

    // simulate parking

    Payment payment = lot.unpark(ticket);

    System.out.println(payment);
  }
}

enum VehicleType {
  MOTORCYCLE,
  CAR,
  SUV,
  TRUCK
}

enum SpotType {
  SMALL,
  MEDIUM,
  LARGE
}

enum TicketStatus {
  ACTIVE,
  CLOSED
}

enum PaymentStatus {
  PENDING,
  COMPLETED,
  FAILED
}

class Vehicle {
  private final String registrationNumber;
  private final VehicleType type;

  public Vehicle(String registrationNumber, VehicleType type) {
    this.registrationNumber = registrationNumber;
    this.type = type;
  }

  public VehicleType getType() {
    return type;
  }

  public String getRegistrationNumber() {
    return registrationNumber;
  }
}

abstract class ParkingSpot {
  protected final String id;
  protected final SpotType type;
  protected boolean occupied;

  protected ParkingSpot(String id, SpotType type) {
    this.id = id;
    this.type = type;
  }

  public boolean isOccupied() {
    return occupied;
  }

  public void occupy() {
    occupied = true;
  }

  public void free() {
    occupied = false;
  }

  public SpotType getType() {
    return type;
  }

  public abstract boolean canFit(Vehicle vehicle);
}

class SmallSpot extends ParkingSpot {

  public SmallSpot(String id) {
    super(id, SpotType.SMALL);
  }

  @Override
  public boolean canFit(Vehicle vehicle) {
    return vehicle.getType() == VehicleType.MOTORCYCLE;
  }
}

class MediumSpot extends ParkingSpot {

  public MediumSpot(String id) {
    super(id, SpotType.MEDIUM);
  }

  @Override
  public boolean canFit(Vehicle vehicle) {
    return vehicle.getType() == VehicleType.CAR
        || vehicle.getType() == VehicleType.SUV;
  }
}

class LargeSpot extends ParkingSpot {

  public LargeSpot(String id) {
    super(id, SpotType.LARGE);
  }

  @Override
  public boolean canFit(Vehicle vehicle) {
    return true;
  }
}

class ParkingTicket {

  private final String ticketId;
  private final Vehicle vehicle;
  private final ParkingSpot spot;
  private final LocalDateTime entryTime;

  private LocalDateTime exitTime;
  private TicketStatus status;

  public ParkingTicket(
      String ticketId,
      Vehicle vehicle,
      ParkingSpot spot) {

    this.ticketId = ticketId;
    this.vehicle = vehicle;
    this.spot = spot;
    this.entryTime = LocalDateTime.now();
    this.status = TicketStatus.ACTIVE;
  }

  public void close() {
    exitTime = LocalDateTime.now();
    status = TicketStatus.CLOSED;
  }

  public Duration getDuration() {
    return Duration.between(entryTime, exitTime);
  }

  public ParkingSpot getSpot() {
    return spot;
  }
}

interface PricingStrategy {
  double calculate(ParkingTicket ticket);
}

class HourlyPricingStrategy
    implements PricingStrategy {

  private static final double RATE = 50;

  @Override
  public double calculate(ParkingTicket ticket) {

    long hours = Math.max(
        1,
        ticket.getDuration().toHours());

    return hours * RATE;
  }
}

record Payment(
    String paymentId,
    double amount,
    PaymentStatus status) {
}

interface SpotAllocationStrategy {

  ParkingSpot allocate(
      Vehicle vehicle,
      List<ParkingSpot> spots);
}

class NearestSpotStrategy
    implements SpotAllocationStrategy {

  @Override
  public ParkingSpot allocate(
      Vehicle vehicle,
      List<ParkingSpot> spots) {

    for (ParkingSpot spot : spots) {
      if (!spot.isOccupied()
          && spot.canFit(vehicle)) {
        return spot;
      }
    }

    return null;
  }
}

class ParkingFloor {

  private final int floorNo;
  private final List<ParkingSpot> spots;

  public ParkingFloor(
      int floorNo,
      List<ParkingSpot> spots) {

    this.floorNo = floorNo;
    this.spots = spots;
  }

  public List<ParkingSpot> getSpots() {
    return spots;
  }
}

class ParkingLot {

  private final String id;

  private final List<ParkingFloor> floors;

  private final SpotAllocationStrategy allocator;

  private final PricingStrategy pricingStrategy;

  public ParkingLot(
      String id,
      List<ParkingFloor> floors,
      SpotAllocationStrategy allocator,
      PricingStrategy pricingStrategy) {

    this.id = id;
    this.floors = floors;
    this.allocator = allocator;
    this.pricingStrategy = pricingStrategy;
  }

  public ParkingTicket park(Vehicle vehicle) {

    for (ParkingFloor floor : floors) {

      ParkingSpot spot = allocator.allocate(
          vehicle,
          floor.getSpots());

      if (spot != null) {

        spot.occupy();

        return new ParkingTicket(
            UUID.randomUUID().toString(),
            vehicle,
            spot);
      }
    }

    throw new RuntimeException(
        "Parking lot full");
  }

  public Payment unpark(
      ParkingTicket ticket) {

    ticket.close();

    ticket.getSpot().free();

    double amount = pricingStrategy.calculate(ticket);

    return new Payment(
        UUID.randomUUID().toString(),
        amount,
        PaymentStatus.COMPLETED);
  }
}
