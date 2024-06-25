// src/components/SidebarLeft.js
import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { ratedMovies, suggestLatestMoviesBasedOnActor, suggestLatestMoviesBasedOnDirector, suggestLatestMoviesBasedOnGenre } from './api';

function SidebarLeft() {
    const location = useLocation();
    const userData = location.state ? location.state : null;
    const [listRatedMovies, setRatedMovies] = useState(null);
    const [listLatestMoviesBasedOnActor, setLatestMoviesBasedOnActor] = useState(null);
    const [listLatestMoviesBasedOnDirector, setLatestMoviesBasedOnDirector] = useState(null);
    const [listLatestMoviesBasedOnGenre, setLatestMoviesBasedOnGenre] = useState(null);
    const handleRatedMovies = async () => {
        try {
          const response = await ratedMovies(userData.user.name);
          setRatedMovies(response);
        } catch (error) {
          console.error('Error during login:', error);
        }
      };
    const handleLatestMoviesBasedOnActor = async () => {
      try {
        const response = await suggestLatestMoviesBasedOnActor(userData.user.name);
        setLatestMoviesBasedOnActor(response);
      } catch (error) {
        console.error('Error during login:', error);
      }
    };
    const handleLatestMoviesBasedOnDirector = async () => {
      try {
        const response = await suggestLatestMoviesBasedOnDirector(userData.user.name);
        setLatestMoviesBasedOnDirector(response);
      } catch (error) {
        console.error('Error during login:', error);
      }
    };
    const handleLatestMoviesBasedOnGenre = async () => {
      try {
        const response = await suggestLatestMoviesBasedOnGenre(userData.user.name);
        setLatestMoviesBasedOnGenre(response);
      } catch (error) {
        console.error('Error during login:', error);
      }
    };

    useEffect(() => {
    // Call handleUserSuggest when the component mounts
    handleRatedMovies();
    handleLatestMoviesBasedOnActor();
    handleLatestMoviesBasedOnDirector();
    handleLatestMoviesBasedOnGenre();
    }, []); 

    return (
        <div className="sidebar sidebar-left">
        <h2>User profile</h2>
        <h4>{userData.user.name}</h4>
        <h2>My favorite movies</h2>
        {listRatedMovies !== null ? (
        listRatedMovies.length > 0 ? (
          <ul className="user-list">
            {listRatedMovies.map((user, index) => (
              <li key={index} className="user-item">
                {user.movie}
              </li>
            ))}
          </ul>
        ) : (
          <p>No rated movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
        <h2>My friends</h2>
        <h2>Quick actions</h2>
        {listLatestMoviesBasedOnActor !== null ? (
        listLatestMoviesBasedOnActor.length > 0 ? (
          <ul className="user-list">
            {listLatestMoviesBasedOnActor.map((user, index) => (
              <li key={index} className="user-item">
                Checkout {user.Actor}'s latest movie!<br/>
                {user.Movie} ({user.Year})
              </li>
            ))}
          </ul>
        ) : (
          <p>No rated movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
      {listLatestMoviesBasedOnDirector !== null ? (
        listLatestMoviesBasedOnDirector.length > 0 ? (
          <ul className="user-list">
            {listLatestMoviesBasedOnDirector.map((user, index) => (
              <li key={index} className="user-item">
                Checkout {user.Director}'s latest movie!<br/>
                {user.Movie} ({user.Year})
              </li>
            ))}
          </ul>
        ) : (
          <p>No rated movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
            {listLatestMoviesBasedOnGenre !== null ? (
        listLatestMoviesBasedOnGenre.length > 0 ? (
          <ul className="user-list">
            {listLatestMoviesBasedOnGenre.map((user, index) => (
              <li key={index} className="user-item">
                Because you love {user.Genre}!<br/>
                {user.Movie} ({user.Year})
              </li>
            ))}
          </ul>
        ) : (
          <p>No rated movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
        {/* Add sidebar content as needed */}
        </div>
    );
    }

export default SidebarLeft;
