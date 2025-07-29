function handleCredentialResponse(response) {
  const token = response.credential;
  localStorage.setItem("google_id_token", token); // Optional: store for dev

  // Show loader popup
  document.getElementById("login-loader").classList.remove("hidden");
  fetch("/auth/verify", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token })
  })
  .then(res => res.json())
  .then(data => {
    if (data.status === "success") {
        // Redirect to dashboard
        window.location.href = "/dashboard";
        console.log(data)
    } else {
      alert("Login failed");
      document.getElementById("login-loader")?.remove();
    }
  })
  .catch(err => {
    document.getElementById("login-loader").classList.add("hidden");
    console.error("Login error:", err);
    // alert("An error occurred. Please try again.");
    // document.getElementById("login-loader")?.remove();
  });
}
