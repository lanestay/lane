import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import Login from "@/components/Login";

// Mock the useAuth hook
const mockLogin = vi.fn();
vi.mock("@/lib/auth", () => ({
  useAuth: () => ({
    login: mockLogin,
    loading: false,
    authenticated: false,
    needsSetup: false,
    user: null,
    logout: vi.fn(),
    refreshStatus: vi.fn(),
  }),
}));

afterEach(() => {
  vi.restoreAllMocks();
  mockLogin.mockReset();
});

describe("Login", () => {
  it("renders the login form", () => {
    render(<Login />);
    expect(screen.getByText("lane")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("you@example.com")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("Password")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sign In" })).toBeInTheDocument();
  });

  it("shows error when submitting empty fields", async () => {
    const user = userEvent.setup();
    render(<Login />);
    await user.click(screen.getByRole("button", { name: "Sign In" }));
    expect(screen.getByText("Email and password are required")).toBeInTheDocument();
  });

  it("calls login on valid credentials", async () => {
    const user = userEvent.setup();
    mockLogin.mockResolvedValueOnce(undefined);
    render(<Login />);
    await user.type(screen.getByPlaceholderText("you@example.com"), "admin@test.com");
    await user.type(screen.getByPlaceholderText("Password"), "secret123");
    await user.click(screen.getByRole("button", { name: "Sign In" }));
    await waitFor(() => expect(mockLogin).toHaveBeenCalledWith("admin@test.com", "secret123"));
  });

  it("shows error on invalid credentials", async () => {
    const user = userEvent.setup();
    mockLogin.mockRejectedValueOnce(new Error("Invalid credentials"));
    render(<Login />);
    await user.type(screen.getByPlaceholderText("you@example.com"), "bad@test.com");
    await user.type(screen.getByPlaceholderText("Password"), "wrong");
    await user.click(screen.getByRole("button", { name: "Sign In" }));
    await waitFor(() => expect(screen.getByText("Invalid credentials")).toBeInTheDocument());
  });

  it("shows loading state while signing in", async () => {
    const user = userEvent.setup();
    // Return a login that never resolves during the test
    mockLogin.mockReturnValueOnce(new Promise(() => {}));
    render(<Login />);
    await user.type(screen.getByPlaceholderText("you@example.com"), "admin@test.com");
    await user.type(screen.getByPlaceholderText("Password"), "secret123");
    await user.click(screen.getByRole("button", { name: "Sign In" }));
    expect(screen.getByText("Signing in...")).toBeInTheDocument();
  });
});
