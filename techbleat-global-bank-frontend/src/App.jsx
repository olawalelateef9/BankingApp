import { useEffect, useMemo, useState } from "react";

const USER_API = "https://techbleatglobalbank.duckdns.org/api/users";
const TX_API = "https://techbleatglobalbank.duckdns.org/api/transactions";
const ACTIVITY_API = "https://techbleatglobalbank.duckdns.org/api/activities";

const currency = new Intl.NumberFormat("en-GB", {
  style: "currency",
  currency: "GBP",
});

export default function TechbleatGlobalBankCustomerApp() {
  const [screen, setScreen] = useState("login");
  const [users, setUsers] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState("");
  const [balance, setBalance] = useState(0);
  const [transactions, setTransactions] = useState([]);
  const [activities, setActivities] = useState([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState({ type: "info", text: "" });

  const [loginForm, setLoginForm] = useState({ userId: "" });
  const [registerForm, setRegisterForm] = useState({
    id: "",
    firstName: "",
    lastName: "",
    email: "",
  });
  const [transferForm, setTransferForm] = useState({
    type: "deposit",
    amount: "",
    toUserId: "",
    reference: "",
  });

  const selectedUser = useMemo(
    () => users.find((user) => user.id === selectedUserId) || null,
    [users, selectedUserId],
  );

  const spendingThisMonth = useMemo(() => {
    return transactions
      .filter((tx) => Number(tx.amount) < 0 || String(tx.transactionType || "").includes("WITHDRAW") || String(tx.transactionType || "").includes("OUT"))
      .reduce((sum, tx) => sum + Math.abs(Number(tx.amount || 0)), 0);
  }, [transactions]);

  const moneyInThisMonth = useMemo(() => {
    return transactions
      .filter((tx) => {
        const type = String(tx.transactionType || "");
        return type.includes("DEPOSIT") || type.includes("IN");
      })
      .reduce((sum, tx) => sum + Number(tx.amount || 0), 0);
  }, [transactions]);

  const safeToSpend = Math.max(balance - 500, 0);

  useEffect(() => {
    void loadUsers();
  }, []);

  useEffect(() => {
    if (selectedUserId) {
      void loadDashboard(selectedUserId);
    }
  }, [selectedUserId]);

  async function api(url, options = {}) {
    const response = await fetch(url, {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    });

    if (!response.ok) {
      let errorText = "Request failed";
      try {
        const data = await response.json();
        errorText = data.detail || data.message || JSON.stringify(data);
      } catch {
        errorText = await response.text();
      }
      throw new Error(errorText || "Request failed");
    }

    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }
    return response.text();
  }

  async function loadUsers() {
    try {
      const data = await api(`${USER_API}/users`);
      setUsers(Array.isArray(data) ? data : []);
    } catch (error) {
      setMessage({ type: "error", text: error.message || "Unable to load users" });
    }
  }

  async function loadDashboard(userId) {
    setLoading(true);
    try {
      const [balanceRes, transactionsRes, activitiesRes] = await Promise.all([
        api(`${TX_API}/balance/${userId}`),
        api(`${TX_API}/transactions/${userId}`),
        api(`${ACTIVITY_API}/activities/${userId}`),
      ]);
      setBalance(Number(balanceRes.balance || 0));
      setTransactions(Array.isArray(transactionsRes) ? transactionsRes : []);
      setActivities(Array.isArray(activitiesRes) ? activitiesRes : []);
      setMessage({ type: "success", text: `Loaded dashboard for ${userId}` });
    } catch (error) {
      setMessage({ type: "error", text: error.message || "Unable to load dashboard" });
    } finally {
      setLoading(false);
    }
  }

  async function handleLogin(event) {
    event.preventDefault();
    if (!loginForm.userId) {
      setMessage({ type: "error", text: "Enter a user ID to continue." });
      return;
    }

    const exists = users.some((user) => user.id === loginForm.userId);
    if (!exists) {
      setMessage({ type: "error", text: "User not found. Please register first." });
      return;
    }

    setSelectedUserId(loginForm.userId);
    setScreen("dashboard");
    setMessage({ type: "success", text: `Welcome back, ${loginForm.userId}` });
  }

  async function handleRegister(event) {
    event.preventDefault();
    const payload = {
      id: registerForm.id.trim(),
      full_name: `${registerForm.firstName} ${registerForm.lastName}`.trim(),
      email: registerForm.email.trim(),
    };

    if (!payload.id || !payload.full_name || !payload.email) {
      setMessage({ type: "error", text: "Complete all registration fields." });
      return;
    }

    try {
      await api(`${USER_API}/users`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      await loadUsers();
      setRegisterForm({ id: "", firstName: "", lastName: "", email: "" });
      setLoginForm({ userId: payload.id });
      setMessage({ type: "success", text: "Account created successfully. You can now sign in." });
      setScreen("login");
    } catch (error) {
      setMessage({ type: "error", text: error.message || "Registration failed" });
    }
  }

  async function handleTransfer(event) {
    event.preventDefault();
    if (!selectedUserId) {
      setMessage({ type: "error", text: "Please sign in first." });
      return;
    }

    const amount = Number(transferForm.amount);
    if (!amount || amount <= 0) {
      setMessage({ type: "error", text: "Enter a valid amount." });
      return;
    }

    try {
      if (transferForm.type === "deposit") {
        await api(`${TX_API}/transactions/deposit`, {
          method: "POST",
          headers: { "X-User-Id": selectedUserId, "Content-Type": "application/json" },
          body: JSON.stringify({ amount }),
        });
      } else if (transferForm.type === "withdraw") {
        await api(`${TX_API}/transactions/withdraw`, {
          method: "POST",
          headers: { "X-User-Id": selectedUserId, "Content-Type": "application/json" },
          body: JSON.stringify({ amount }),
        });
      } else {
        if (!transferForm.toUserId.trim()) {
          setMessage({ type: "error", text: "Enter a destination user ID." });
          return;
        }
        await api(`${TX_API}/transactions/transfer`, {
          method: "POST",
          headers: { "X-User-Id": selectedUserId, "Content-Type": "application/json" },
          body: JSON.stringify({
            toUserId: transferForm.toUserId.trim(),
            amount,
          }),
        });
      }

      setTransferForm({ type: "deposit", amount: "", toUserId: "", reference: "" });
      await loadDashboard(selectedUserId);
      setScreen("dashboard");
      setMessage({ type: "success", text: "Transaction completed successfully." });
    } catch (error) {
      setMessage({ type: "error", text: error.message || "Transaction failed" });
    }
  }

  function handleLogout() {
    setSelectedUserId("");
    setLoginForm({ userId: "" });
    setTransactions([]);
    setActivities([]);
    setBalance(0);
    setScreen("login");
    setMessage({ type: "info", text: "You have been signed out." });
  }

  const accounts = [
    {
      name: "Everyday Account",
      number: selectedUser ? `•••• ${selectedUser.id.slice(-4).padStart(4, "0")}` : "•••• 2048",
      balance: currency.format(balance),
    },
    {
      name: "Savings Account",
      number: "•••• 8831",
      balance: currency.format(Math.max(balance * 0.6, 0)),
    },
  ];

  const quickActions = [
    { label: "Deposit Funds", screen: "transfer", mode: "deposit" },
    { label: "Withdraw Funds", screen: "transfer", mode: "withdraw" },
    { label: "Transfer Money", screen: "transfer", mode: "transfer" },
    { label: "View Reports", screen: "report" },
  ];

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <TopNav
          screen={screen}
          setScreen={setScreen}
          selectedUser={selectedUser}
          onLogout={handleLogout}
        />

        {message.text ? <Banner message={message} /> : null}

        {screen === "login" && (
          <LoginScreen
            loginForm={loginForm}
            setLoginForm={setLoginForm}
            onLogin={handleLogin}
            onGoRegister={() => setScreen("register")}
            users={users}
          />
        )}

        {screen === "register" && (
          <RegisterScreen
            registerForm={registerForm}
            setRegisterForm={setRegisterForm}
            onRegister={handleRegister}
            onGoLogin={() => setScreen("login")}
          />
        )}

        {screen === "dashboard" && (
          <DashboardScreen
            selectedUser={selectedUser}
            accounts={accounts}
            balance={balance}
            quickActions={quickActions}
            onQuickAction={(action) => {
              if (action.mode) {
                setTransferForm((prev) => ({ ...prev, type: action.mode }));
              }
              setScreen(action.screen);
            }}
            transactions={transactions}
            spendingThisMonth={spendingThisMonth}
            moneyInThisMonth={moneyInThisMonth}
            safeToSpend={safeToSpend}
            loading={loading}
          />
        )}

        {screen === "transfer" && (
          <TransferScreen
            selectedUser={selectedUser}
            transferForm={transferForm}
            setTransferForm={setTransferForm}
            onSubmit={handleTransfer}
            users={users}
            balance={balance}
          />
        )}

        {screen === "report" && (
          <ReportScreen
            selectedUser={selectedUser}
            balance={balance}
            transactions={transactions}
            activities={activities}
            moneyInThisMonth={moneyInThisMonth}
            spendingThisMonth={spendingThisMonth}
            safeToSpend={safeToSpend}
          />
        )}
      </div>
    </div>
  );
}

function TopNav({ screen, setScreen, selectedUser, onLogout }) {
  const nav = ["login", "register", "dashboard", "transfer", "report"];

  return (
    <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
      <div className="flex flex-wrap gap-3">
        {nav.map((item) => (
          <button
            key={item}
            type="button"
            onClick={() => setScreen(item)}
            className={`rounded-2xl px-4 py-2 text-sm capitalize transition ${
              item === screen
                ? "bg-gradient-to-r from-cyan-400 to-blue-600 text-slate-950"
                : "border border-white/10 bg-white/5 text-slate-300 hover:bg-white/10"
            }`}
          >
            {item}
          </button>
        ))}
      </div>

      <div className="flex items-center gap-3">
        {selectedUser ? (
          <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300">
            Signed in as <span className="font-semibold text-white">{selectedUser.full_name}</span>
          </div>
        ) : null}
        {selectedUser ? (
          <button
            type="button"
            onClick={onLogout}
            className="rounded-2xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300 hover:bg-white/10"
          >
            Logout
          </button>
        ) : null}
      </div>
    </div>
  );
}

function Banner({ message }) {
  const tones = {
    success: "border-emerald-400/20 bg-emerald-500/10 text-emerald-200",
    error: "border-rose-400/20 bg-rose-500/10 text-rose-200",
    info: "border-cyan-400/20 bg-cyan-500/10 text-cyan-200",
  };

  return (
    <div className={`mb-6 rounded-2xl border px-4 py-3 text-sm ${tones[message.type] || tones.info}`}>
      {message.text}
    </div>
  );
}

function Shell({ children, title, subtitle, aside }) {
  return (
    <div className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
      <div className="rounded-[32px] border border-white/10 bg-slate-900/85 p-8 shadow-2xl shadow-black/30">
        <p className="text-sm uppercase tracking-[0.25em] text-cyan-300">Techbleat Global Bank</p>
        <h1 className="mt-3 text-4xl font-semibold">{title}</h1>
        <p className="mt-3 max-w-xl text-sm leading-6 text-slate-400">{subtitle}</p>
        <div className="mt-8">{children}</div>
      </div>
      <div className="rounded-[32px] border border-white/10 bg-gradient-to-br from-slate-900 via-blue-950/60 to-cyan-950/40 p-8 shadow-2xl shadow-black/30">
        {aside}
      </div>
    </div>
  );
}

function LoginScreen({ loginForm, setLoginForm, onLogin, onGoRegister, users }) {
  return (
    <Shell
      title="Welcome back"
      subtitle="Sign in with a registered user ID to explore the customer banking experience."
      aside={
        <div className="flex h-full flex-col justify-between">
          <div>
            <div className="flex h-16 w-16 items-center justify-center rounded-3xl bg-gradient-to-br from-cyan-400 to-blue-600 text-2xl font-bold text-slate-950 shadow-lg shadow-cyan-500/30">T</div>
            <h2 className="mt-8 text-3xl font-semibold">Secure access to your money</h2>
            <p className="mt-4 text-sm leading-7 text-slate-300">
              Check balances, move money, view activity and download reports from one elegant banking dashboard.
            </p>
          </div>
          <div className="grid gap-4">
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200">
              Registered demo users: <span className="font-semibold">{users.length}</span>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200">
              Use an ID from registration, such as <span className="font-semibold">user1</span>
            </div>
          </div>
        </div>
      }
    >
      <form className="space-y-5" onSubmit={onLogin}>
        <FormField
          label="User ID"
          placeholder="Enter your user ID"
          value={loginForm.userId}
          onChange={(value) => setLoginForm({ userId: value })}
        />
        <div>
          <label className="mb-2 block text-sm text-slate-300">Password</label>
          <input
            type="password"
            className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-white outline-none placeholder:text-slate-500"
            placeholder="Demo mode only"
            disabled
          />
        </div>
        <button className="w-full rounded-2xl bg-gradient-to-r from-cyan-400 to-blue-600 px-5 py-4 font-medium text-slate-950 shadow-lg shadow-cyan-500/25">
          Sign In
        </button>
        <p className="text-center text-sm text-slate-400">
          Need an account?{" "}
          <button type="button" onClick={onGoRegister} className="text-cyan-300">
            Create one now
          </button>
        </p>
      </form>
    </Shell>
  );
}

function RegisterScreen({ registerForm, setRegisterForm, onRegister, onGoLogin }) {
  return (
    <Shell
      title="Create your account"
      subtitle="Register a new customer profile backed by FastAPI and PostgreSQL."
      aside={
        <div className="flex h-full flex-col justify-between">
          <div className="rounded-[30px] border border-white/10 bg-white/5 p-6">
            <p className="text-sm text-cyan-300">Why customers will like it</p>
            <h3 className="mt-3 text-2xl font-semibold">Simple onboarding</h3>
            <p className="mt-3 text-sm leading-7 text-slate-300">
              Clean forms, friendly spacing and a reassuring visual tone that feels trustworthy and modern.
            </p>
          </div>
          <div className="grid gap-4">
            {[
              "Fast account setup",
              "Creates user and account records",
              "Ready for immediate sign-in",
            ].map((item) => (
              <div key={item} className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200">
                {item}
              </div>
            ))}
          </div>
        </div>
      }
    >
      <form onSubmit={onRegister}>
        <div className="grid gap-5 md:grid-cols-2">
          <FormField
            label="User ID"
            placeholder="user1"
            value={registerForm.id}
            onChange={(value) => setRegisterForm((prev) => ({ ...prev, id: value }))}
          />
          <div />
          <FormField
            label="First name"
            placeholder="Alice"
            value={registerForm.firstName}
            onChange={(value) => setRegisterForm((prev) => ({ ...prev, firstName: value }))}
          />
          <FormField
            label="Last name"
            placeholder="Johnson"
            value={registerForm.lastName}
            onChange={(value) => setRegisterForm((prev) => ({ ...prev, lastName: value }))}
          />
          <FormField
            label="Email"
            placeholder="alice@example.com"
            value={registerForm.email}
            onChange={(value) => setRegisterForm((prev) => ({ ...prev, email: value }))}
          />
          <div>
            <label className="mb-2 block text-sm text-slate-300">Preferred account type</label>
            <select className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-white outline-none">
              <option>Everyday Account</option>
              <option>Savings Account</option>
            </select>
          </div>
        </div>
        <button className="mt-6 w-full rounded-2xl bg-gradient-to-r from-cyan-400 to-blue-600 px-5 py-4 font-medium text-slate-950 shadow-lg shadow-cyan-500/25">
          Open My Account
        </button>
        <p className="mt-4 text-center text-sm text-slate-400">
          Already registered?{" "}
          <button type="button" onClick={onGoLogin} className="text-cyan-300">
            Go to sign in
          </button>
        </p>
      </form>
    </Shell>
  );
}

function DashboardScreen({
  selectedUser,
  accounts,
  balance,
  quickActions,
  onQuickAction,
  transactions,
  spendingThisMonth,
  moneyInThisMonth,
  safeToSpend,
  loading,
}) {
  return (
    <>
      <header className="mb-8 flex flex-col gap-4 rounded-[32px] border border-white/10 bg-gradient-to-r from-slate-900 to-slate-800 px-6 py-6 shadow-2xl shadow-black/30 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-4">
          <div className="flex h-14 w-14 items-center justify-center rounded-3xl bg-gradient-to-br from-cyan-400 to-blue-600 text-xl font-bold text-slate-950 shadow-lg shadow-cyan-500/20">
            T
          </div>
          <div>
            <p className="text-sm uppercase tracking-[0.25em] text-cyan-300">Mobile & Web Banking</p>
            <h1 className="mt-1 text-3xl font-semibold">Techbleat Global Bank</h1>
            <p className="mt-2 text-sm text-slate-400">
              {selectedUser ? `Welcome back, ${selectedUser.full_name}` : "Please sign in to view your dashboard."}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-300">
            Current balance <span className="font-medium text-white">{currency.format(balance)}</span>
          </div>
          <button
            className="rounded-2xl bg-gradient-to-r from-cyan-400 to-blue-600 px-5 py-3 text-sm font-medium text-slate-950 shadow-lg shadow-cyan-500/25"
            onClick={() => onQuickAction({ screen: "transfer", mode: "transfer" })}
          >
            Send Money
          </button>
        </div>
      </header>

      <section className="mb-8 rounded-[32px] border border-white/10 bg-gradient-to-br from-cyan-500/15 via-slate-900 to-blue-600/10 p-7 shadow-2xl shadow-black/20">
        <p className="text-sm text-cyan-200">Available Balance</p>
        <h2 className="mt-3 text-5xl font-semibold tracking-tight">{currency.format(balance)}</h2>
        <p className="mt-3 text-sm text-slate-300">Across your personal accounts</p>

        <div className="mt-8 grid gap-4 md:grid-cols-2">
          {accounts.map((account) => (
            <div key={account.name} className="rounded-[28px] border border-white/10 bg-white/5 p-5 backdrop-blur">
              <p className="text-sm text-slate-400">{account.name}</p>
              <p className="mt-1 text-sm text-slate-500">{account.number}</p>
              <h3 className="mt-4 text-2xl font-semibold">{account.balance}</h3>
            </div>
          ))}
        </div>
      </section>

      <section className="mb-8 grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <div className="rounded-[32px] border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/20">
          <div className="mb-5 flex items-center justify-between">
            <h3 className="text-xl font-semibold">Quick Actions</h3>
            <span className="text-sm text-slate-400">Everyday banking</span>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            {quickActions.map((action) => (
              <button
                key={action.label}
                onClick={() => onQuickAction(action)}
                className="rounded-2xl border border-white/10 bg-white/5 px-4 py-5 text-left text-sm font-medium text-slate-100 transition hover:bg-white/10"
              >
                {action.label}
              </button>
            ))}
          </div>

          <div className="mt-6 rounded-[28px] border border-emerald-400/20 bg-emerald-500/10 p-5">
            <p className="text-sm text-emerald-300">Savings Goal</p>
            <h4 className="mt-2 text-2xl font-semibold">Emergency Fund</h4>
            <p className="mt-2 text-sm text-slate-300">{currency.format(Math.min(balance, 5000))} of {currency.format(5000)} saved</p>
            <div className="mt-4 h-3 overflow-hidden rounded-full bg-white/10">
              <div className="h-full rounded-full bg-gradient-to-r from-emerald-400 to-cyan-400" style={{ width: `${Math.min((balance / 5000) * 100, 100)}%` }} />
            </div>
          </div>
        </div>

        <div className="rounded-[32px] border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/20">
          <div className="mb-5 flex items-center justify-between">
            <h3 className="text-xl font-semibold">Cards</h3>
            <button className="rounded-xl border border-white/10 px-3 py-2 text-sm text-slate-300 hover:bg-white/5">
              Manage Cards
            </button>
          </div>

          <div className="rounded-[30px] bg-gradient-to-br from-slate-800 via-blue-950 to-cyan-900 p-6 shadow-xl shadow-cyan-900/20">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-xs uppercase tracking-[0.25em] text-cyan-200">Platinum Debit</p>
                <p className="mt-8 text-2xl tracking-[0.3em] text-white">**** **** **** {selectedUser ? selectedUser.id.slice(-4).padStart(4, "0") : "2048"}</p>
              </div>
              <div className="rounded-full bg-white/10 px-3 py-1 text-xs text-slate-200">Active</div>
            </div>
            <div className="mt-10 flex items-end justify-between">
              <div>
                <p className="text-xs text-slate-300">Card Holder</p>
                <p className="mt-1 font-medium">{selectedUser?.full_name || "Customer"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-300">Valid Thru</p>
                <p className="mt-1 font-medium">09/29</p>
              </div>
            </div>
          </div>

          <div className="mt-5 grid grid-cols-2 gap-4">
            <MetricCard title="Money In" value={currency.format(moneyInThisMonth)} tone="text-emerald-300" />
            <MetricCard title="Spent This Month" value={currency.format(spendingThisMonth)} tone="text-white" />
          </div>
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
        <div className="rounded-[32px] border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/20">
          <div className="mb-5 flex items-center justify-between">
            <h3 className="text-xl font-semibold">Recent Transactions</h3>
            <button className="rounded-xl border border-white/10 px-3 py-2 text-sm text-slate-300 hover:bg-white/5">
              View Statement
            </button>
          </div>

          {loading ? <p className="text-sm text-slate-400">Loading transactions…</p> : null}

          <div className="space-y-4">
            {transactions.length === 0 ? (
              <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-sm text-slate-400">
                No transactions yet.
              </div>
            ) : (
              transactions.slice(0, 6).map((tx) => {
                const type = String(tx.transactionType || "");
                const isCredit = type.includes("DEPOSIT") || type.includes("IN");
                return (
                  <div key={`${tx.id}-${type}`} className="flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-4">
                    <div>
                      <p className="font-medium">{niceType(type)}</p>
                      <p className="mt-1 text-sm text-slate-400">{tx.reference || "Techbleat Global Bank"}</p>
                    </div>
                    <div className="text-right">
                      <p className={`font-semibold ${isCredit ? "text-emerald-300" : "text-white"}`}>
                        {isCredit ? "+" : "-"}{currency.format(Math.abs(Number(tx.amount || 0))).replace("£", "£")}
                      </p>
                      <p className="mt-1 text-sm text-slate-400">{formatDate(tx.createdAt)}</p>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>

        <div className="rounded-[32px] border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/20">
          <div className="mb-5 flex items-center justify-between">
            <h3 className="text-xl font-semibold">Insights</h3>
            <span className="text-sm text-slate-400">Personal overview</span>
          </div>

          <div className="space-y-4">
            <MetricCard title="Income This Month" value={currency.format(moneyInThisMonth)} tone="text-emerald-300" />
            <MetricCard title="Bills & Transfers" value={currency.format(spendingThisMonth)} tone="text-white" />
            <MetricCard title="Safe to Spend" value={currency.format(safeToSpend)} tone="text-white" />
          </div>
        </div>
      </section>
    </>
  );
}

function TransferScreen({ selectedUser, transferForm, setTransferForm, onSubmit, users, balance }) {
  const transferModes = [
    { id: "deposit", label: "Deposit" },
    { id: "withdraw", label: "Withdraw" },
    { id: "transfer", label: "Transfer" },
  ];

  return (
    <Shell
      title="Move money"
      subtitle="Use the real transaction APIs to deposit, withdraw or transfer funds."
      aside={
        <div className="space-y-5">
          <div className="rounded-[28px] border border-white/10 bg-white/5 p-6">
            <p className="text-sm text-slate-400">From</p>
            <h3 className="mt-2 text-2xl font-semibold">{selectedUser?.full_name || "No user selected"}</h3>
            <p className="mt-2 text-sm text-slate-300">Available: {currency.format(balance)}</p>
          </div>
          <div className="rounded-[28px] border border-emerald-400/20 bg-emerald-500/10 p-6">
            <p className="text-sm text-emerald-300">Transaction Summary</p>
            <div className="mt-4 space-y-3 text-sm text-slate-200">
              <div className="flex justify-between"><span>Mode</span><span>{niceType(transferForm.type)}</span></div>
              <div className="flex justify-between"><span>Amount</span><span>{currency.format(Number(transferForm.amount || 0))}</span></div>
              <div className="flex justify-between"><span>Fee</span><span>{currency.format(0)}</span></div>
              <div className="flex justify-between border-t border-white/10 pt-3 font-medium"><span>Total</span><span>{currency.format(Number(transferForm.amount || 0))}</span></div>
            </div>
          </div>
        </div>
      }
    >
      <form className="space-y-5" onSubmit={onSubmit}>
        <div className="grid gap-3 sm:grid-cols-3">
          {transferModes.map((mode) => (
            <button
              key={mode.id}
              type="button"
              onClick={() => setTransferForm((prev) => ({ ...prev, type: mode.id }))}
              className={`rounded-2xl border px-4 py-4 text-sm font-medium ${
                transferForm.type === mode.id
                  ? "border-cyan-300 bg-cyan-400/15 text-cyan-200"
                  : "border-white/10 bg-white/5 text-slate-300"
              }`}
            >
              {mode.label}
            </button>
          ))}
        </div>

        {transferForm.type === "transfer" ? (
          <div>
            <label className="mb-2 block text-sm text-slate-300">Destination user</label>
            <select
              value={transferForm.toUserId}
              onChange={(event) => setTransferForm((prev) => ({ ...prev, toUserId: event.target.value }))}
              className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-white outline-none"
            >
              <option value="">Select a beneficiary</option>
              {users
                .filter((user) => user.id !== selectedUser?.id)
                .map((user) => (
                  <option key={user.id} value={user.id}>{user.full_name} ({user.id})</option>
                ))}
            </select>
          </div>
        ) : null}

        <FormField
          label="Amount"
          placeholder="0.00"
          value={transferForm.amount}
          onChange={(value) => setTransferForm((prev) => ({ ...prev, amount: value }))}
          type="number"
        />

        <div>
          <label className="mb-2 block text-sm text-slate-300">Reference</label>
          <textarea
            value={transferForm.reference}
            onChange={(event) => setTransferForm((prev) => ({ ...prev, reference: event.target.value }))}
            className="h-28 w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-white outline-none placeholder:text-slate-500"
            placeholder="Payment reference"
          />
        </div>

        <button className="w-full rounded-2xl bg-gradient-to-r from-cyan-400 to-blue-600 px-5 py-4 font-medium text-slate-950 shadow-lg shadow-cyan-500/25">
          Confirm Transaction
        </button>
      </form>
    </Shell>
  );
}

function ReportScreen({ selectedUser, balance, transactions, activities, moneyInThisMonth, spendingThisMonth, safeToSpend }) {
  const weeklyBars = buildWeeklyBars(transactions);

  return (
    <Shell
      title="Statements & reports"
      subtitle="A customer-friendly reporting page for viewing balance trends, transactions and activity."
      aside={
        <div className="space-y-5">
          <div className="rounded-[28px] border border-white/10 bg-white/5 p-6">
            <p className="text-sm text-slate-400">Customer</p>
            <h3 className="mt-2 text-3xl font-semibold">{selectedUser?.full_name || "No user selected"}</h3>
            <p className="mt-3 text-sm text-slate-300">Live report snapshot from your local proof of concept.</p>
          </div>
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-1">
            <MetricCard title="Money In" value={currency.format(moneyInThisMonth)} tone="text-emerald-300" />
            <MetricCard title="Money Out" value={currency.format(spendingThisMonth)} tone="text-white" />
          </div>
        </div>
      }
    >
      <div className="grid gap-5 md:grid-cols-2">
        <div className="rounded-[28px] border border-white/10 bg-white/5 p-5">
          <p className="text-sm text-slate-400">Available balance</p>
          <h3 className="mt-2 text-xl font-semibold">{currency.format(balance)}</h3>
        </div>
        <div className="rounded-[28px] border border-white/10 bg-white/5 p-5">
          <p className="text-sm text-slate-400">Safe to spend</p>
          <h3 className="mt-2 text-xl font-semibold">{currency.format(safeToSpend)}</h3>
        </div>
      </div>

      <div className="mt-5 rounded-[28px] border border-white/10 bg-white/5 p-6">
        <div className="mb-5 flex items-center justify-between">
          <h3 className="text-lg font-semibold">Weekly transaction pattern</h3>
          <span className="text-sm text-slate-400">Last 7 groups</span>
        </div>
        <div className="flex items-end justify-between gap-3">
          {weeklyBars.map((bar, idx) => (
            <div key={`${bar.label}-${idx}`} className="flex flex-1 flex-col items-center gap-3">
              <div className="w-full rounded-t-2xl bg-gradient-to-t from-cyan-500 to-blue-500" style={{ height: `${bar.height}px`, minHeight: "16px" }} />
              <span className="text-xs text-slate-500">{bar.label}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-5 grid gap-4 sm:grid-cols-2">
        <MetricCard title="Transactions" value={String(transactions.length)} tone="text-white" />
        <MetricCard title="Activity Events" value={String(activities.length)} tone="text-white" />
      </div>

      <div className="mt-5 grid gap-4 sm:grid-cols-2">
        <button className="rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-left font-medium text-white">Download PDF Statement</button>
        <button className="rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-left font-medium text-white">Export CSV Report</button>
      </div>
    </Shell>
  );
}

function FormField({ label, placeholder, type = "text", value, onChange }) {
  return (
    <div>
      <label className="mb-2 block text-sm text-slate-300">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(event) => onChange?.(event.target.value)}
        className="w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-white outline-none placeholder:text-slate-500"
        placeholder={placeholder}
      />
    </div>
  );
}

function MetricCard({ title, value, tone }) {
  return (
    <div className="rounded-[24px] border border-white/10 bg-white/5 p-5">
      <p className="text-sm text-slate-400">{title}</p>
      <h4 className={`mt-2 text-3xl font-semibold ${tone}`}>{value}</h4>
    </div>
  );
}

function niceType(type) {
  return String(type || "")
    .toLowerCase()
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatDate(value) {
  if (!value) return "Recent";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Recent";
  return date.toLocaleString("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function buildWeeklyBars(transactions) {
  const source = transactions.slice(0, 7).reverse();
  if (source.length === 0) {
    return Array.from({ length: 7 }, (_, index) => ({ label: `W${index + 1}`, height: 20 + index * 8 }));
  }

  const maxAmount = Math.max(...source.map((tx) => Math.abs(Number(tx.amount || 0))), 1);
  return source.map((tx, index) => ({
    label: `W${index + 1}`,
    height: Math.max((Math.abs(Number(tx.amount || 0)) / maxAmount) * 180, 20),
  }));
}
