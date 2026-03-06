// LoginPage.js
import React from 'react';
import './LoginPage.css';  // Optional: if you want to have separate styles for this page

const API_BASE_URL =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.REACT_APP_API_BASE_URL ||
    'http://127.0.0.1:8080';

function LoginPage() {
    const initiateLogin = () => {
        // Redirect the user to the server's /login endpoint to start the OAuth process.
        window.location.href = `${API_BASE_URL}/login`;

    };

    return (
        <div className="login-page">
            <h2>Welcome to Splitify</h2>
            <p>To get started, please login with your Spotify account:</p>
            <button onClick={initiateLogin}>Login with Spotify</button>
        </div>
    );
}

export default LoginPage;
