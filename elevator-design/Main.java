
/**
 * Design an elevator control system for a building. The system should handle
 * multiple elevators, floor requests, and move elevators efficiently to service
 * requests.
 * 1. Primary Capabilities
 * - How many elevators and floors? Fixed or configurable?
 * - Are hall calls up and down or do they choose floor ?
 * - Can passenger select multiple destinatoin floors inside elevators
 * - The problem says the cars should move efficiently, what does that mean?
 * 2. Error Handling
 * - What about invalid floor requests
 * - What if someone request floor they are already in
 * 3. Scope boundaries
 * - Capacity, weight boundaries, door mechanics, emergency stop
 * - Simmulation (step/tick) or actual hardware control software
 *
 */
import java.util.*;

public class Main {
  public static void main(String[] args) {
    System.out.println("Hello, World!!");
  }
}

enum Direction {
  UP, DOWN, IDLE;
}

// Entities:
class Elevator {
  private int floor;
  private Direction direction;
  private Set<Request> requests;

  public Elevator(int floor) {
    this.floor = floor;
    direction = Direction.IDLE;
    requests = new HashSet<>();
  }

  public boolean addRequest(Request request) {
    return this.requests.add(request);
  }

  public void step() {
    if (null == requests || requests.isEmpty()) {
      direction = Direction.IDLE;
    }
    if (Direction.IDLE.equals(direction)) {
      var nearestRequest = findNearestReqeust();
      this.direction = nearestRequest.getFloor() > floor ? Direction.UP : Direction.DOWN;
    }

    var requestType = Direction.UP.equals(this.direction) ? RequestType.PickUp : RequestType.PickDown;
    var request = new Request(floor, requestType);
    if (this.requests.contains(request)) {
      requests.remove(request);
      stop();
      return;
    }

    if (!hasRequestAhead()) {
      this.direction = Direction.UP.equals(direction) ? Direction.DOWN : Direction.UP;
    }
    if (Direction.UP.equals(this.direction)) {
      if (floor + 1 < ElevatorController.MAX_FLOORS) {
        floor++;
      } else {
        this.direction = Direction.DOWN;
      }
    } else {
      if (floor > 1) {
        floor--;
      } else {
        this.direction = Direction.UP;
      }
    }

  }

  private boolean hasRequestAhead() {
    for (var r : requests) {
      if ((Direction.UP.equals(this.direction) && RequestType.PickUp.equals(r.requestType()) && r.floor() >= this.floor)
          || (Direction.DOWN.equals(this.direction) && RequestType.PickDown.equals(r.requestType())
              && r.floor() <= this.floor)) {
        return true;
      }
    }
    return false;
  }

  private void stop() {
  }

  private Request findNearestReqeust() {
    int minDist = ElevatorController.MAX_FLOORS;
    Request request = null;
    for (var r : requests) {
      var dist = Math.abs(this.floor - r.floor());
      if (dist < minDist) {
        request = r;
      }
    }
    return request;
  }

  public int getFloor() {
    return this.floor;
  }

  public Direction getDirection() {
    return this.direction;
  }
}

// Floor (number)
record Request(int floor, RequestType requestType) {
}

enum RequestType {
  PickUp, PickDown, Destination;
}

class ElevatorException extends Exception {
  public ElevatorException(String message) {
    super(message);
  }
}

class ElevatorController {
  public static final int MAX_FLOORS = 9;
  private List<Elevator> elevators;

  public ElevatorController() {
    for (int i = 1; i <= MAX_FLOORS; ++i) {
      elevators.add(new Elevator(i));
    }
  }

  public boolean requestElevator(int floor, RequestType requestType) throws ElevatorException {
    if (floor <= 0 || floor > MAX_FLOORS) {
      throw new ElevatorException("Invalid floor: " + floor);
    }
    var request = new Request(floor, requestType);
    var elevator = selectElevator(request);
    return elevator.addRequest(request);
  }

  public Elevator selectElevator(Request request) throws ElevatorException {
    int floor = request.floor();
    Direction direction = null;
    switch (request.requestType()) {
      case Destination -> {
        throw new ElevatorException("Invalid request. Called request elevator with destination");
      }
      case PickDown -> {
        direction = Direction.DOWN;
      }
      case PickUp -> {
        direction = Direction.UP;
      }
    }
    Elevator best = selectInSameDirectionOrIdleNotCrossedFloor(direction, floor);
    if (best != null) {
      return best;
    }
    best = findNearest(floor);
    return best;
  }

  private Elevator findNearest(int floor) {
    Elevator nearest = elevators.get(0);
    int maxDist = MAX_FLOORS;
    for (Elevator e : this.elevators) {
      if (Math.abs(e.getFloor() - floor) < maxDist) {
        nearest = e;
      }
    }
    return nearest;
  }

  private Elevator selectInSameDirectionOrIdleNotCrossedFloor(Direction direction, int floor) {
    Elevator elevator = null;
    List<Elevator> lists = new ArrayList<>();
    if (Direction.DOWN.equals(direction)) {
      lists = this.elevators.stream().filter(e -> e.getDirection().equals(direction) && e.getFloor() >= floor)
          .toList();

    } else if (Direction.UP.equals(direction)) {
      lists = this.elevators.stream().filter(e -> e.getDirection().equals(direction) && e.getFloor() <= floor)
          .toList();
    }
    int minDist = MAX_FLOORS;
    for (Elevator e : lists) {
      if (minDist < Math.abs(floor - e.getFloor())) {
        elevator = e;
        minDist = Math.abs(floor - e.getFloor());
      }
    }
    return elevator;
  }

  private void step() {
    for (var e : elevators) {
      e.step();
    }
  }
}
