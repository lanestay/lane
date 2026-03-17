import { render, screen } from "@testing-library/react";
import ResultsTable, { ErrorBanner } from "@/components/ResultsTable";
import { QUERY_RESULT } from "../helpers";

describe("ResultsTable", () => {
  it("renders column headers from metadata", () => {
    render(<ResultsTable result={QUERY_RESULT} />);
    expect(screen.getByText("id")).toBeInTheDocument();
    expect(screen.getByText("name")).toBeInTheDocument();
  });

  it("renders data rows", () => {
    render(<ResultsTable result={QUERY_RESULT} />);
    expect(screen.getByText("Alice")).toBeInTheDocument();
    expect(screen.getByText("Bob")).toBeInTheDocument();
  });

  it("renders stats bar", () => {
    render(<ResultsTable result={QUERY_RESULT} />);
    expect(screen.getByText("2 rows")).toBeInTheDocument();
    expect(screen.getByText("15ms")).toBeInTheDocument();
  });
});

describe("ErrorBanner", () => {
  it("renders the error message", () => {
    render(<ErrorBanner message="Something went wrong" />);
    expect(screen.getByText("Something went wrong")).toBeInTheDocument();
  });
});
